# src/aws/s3_client.py

import hashlib
import io
import os
import re
from dataclasses import dataclass
from datetime import datetime

import boto3
import pandas as pd
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
)

from config.endpoints import ENDPOINTS

from src.common.exceptions import (
    ConfigurationError,
    DataValidationError,
    S3UploadError,
    S3ValidationError,
)

from src.common.logging_config import (
    get_logger,
)


logger = get_logger(__name__)


# ============================================================
# UPLOAD RESULT
# ============================================================

@dataclass
class S3UploadResult:
    """
    Metadata returned after successful S3 processing.
    """

    entity_name: str

    study_id: str

    uploaded: bool

    record_count: int

    bucket_name: str | None

    s3_key: str | None

    s3_uri: str | None

    checksum: str | None

    file_size_bytes: int

    run_id: str

    load_date: str


# ============================================================
# ENVIRONMENT CONFIGURATION
# ============================================================

S3_BUCKET_NAME = os.getenv(
    "S3_BUCKET_NAME"
)

AWS_REGION = os.getenv(
    "AWS_DEFAULT_REGION",
    "ap-south-1",
)


# ============================================================
# CONFIGURATION VALIDATION
# ============================================================

def _validate_configuration() -> None:
    """
    Validate mandatory S3 configuration.
    """

    if not S3_BUCKET_NAME:

        raise ConfigurationError(
            "S3_BUCKET_NAME environment variable is missing"
        )


# ============================================================
# ENTITY VALIDATION
# ============================================================

def _validate_entity(
    entity_name: str,
) -> dict:
    """
    Validate entity exists in endpoint configuration.
    """

    if entity_name not in ENDPOINTS:

        raise ConfigurationError(
            f"Unknown entity={entity_name}"
        )

    return ENDPOINTS[
        entity_name
    ]


# ============================================================
# STUDY ID SANITIZATION
# ============================================================

def _sanitize_study_id(
    study_id: str,
) -> str:
    """
    Make study_id safe for S3 key.

    Example:

        ONC101

    remains:

        ONC101
    """

    if not study_id:

        raise ConfigurationError(
            "study_id cannot be empty"
        )

    cleaned = re.sub(
        r"[^A-Za-z0-9_.\-]",
        "-",
        study_id.strip(),
    )

    if not cleaned:

        raise ConfigurationError(
            (
                "study_id became empty "
                "after sanitization"
            )
        )

    return cleaned


# ============================================================
# RUN ID SANITIZATION
# ============================================================

def _sanitize_run_id(
    run_id: str,
) -> str:
    """
    Convert Airflow run_id into an S3-safe value.

    Example:

        scheduled__2026-08-16T10:00:00+00:00

    becomes:

        scheduled__2026-08-16T10-00-00+00-00
    """

    if not run_id:

        raise ConfigurationError(
            "run_id cannot be empty"
        )

    cleaned = re.sub(
        r"[^A-Za-z0-9_.+\-=]",
        "-",
        run_id.strip(),
    )

    return cleaned


# ============================================================
# LOAD DATE VALIDATION
# ============================================================

def _validate_load_date(
    load_date: str,
) -> str:
    """
    Ensure load_date follows YYYY-MM-DD.
    """

    try:

        datetime.strptime(
            load_date,
            "%Y-%m-%d",
        )

        return load_date

    except ValueError as exc:

        raise ConfigurationError(
            (
                "load_date must use "
                "YYYY-MM-DD format. "
                f"Received={load_date}"
            )
        ) from exc


# ============================================================
# STUDY PARTITION VALIDATION
# ============================================================

def _validate_study_partition(
    entity_name: str,
    study_id: str,
    df: pd.DataFrame,
) -> None:
    """
    Protect against accidentally writing records
    from another study into the wrong S3 partition.

    Example:

        requested partition = ONC101

    but DataFrame contains:

        ONC101
        ONC102

    The upload must fail.
    """

    if df.empty:
        return


    # --------------------------------------------------------
    # study_id must exist
    # --------------------------------------------------------

    if "study_id" not in df.columns:

        raise DataValidationError(
            (
                "study_id column missing "
                f"for entity={entity_name}"
            )
        )


    # --------------------------------------------------------
    # NULL study IDs are not allowed
    # --------------------------------------------------------

    if df["study_id"].isna().any():

        raise DataValidationError(
            (
                "NULL study_id found "
                f"for entity={entity_name}"
            )
        )


    # --------------------------------------------------------
    # Collect unique study IDs in DataFrame
    # --------------------------------------------------------

    dataframe_studies = (
        df["study_id"]
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )


    # --------------------------------------------------------
    # We expect exactly one study in each upload
    # --------------------------------------------------------

    if len(dataframe_studies) != 1:

        logger.error(
            (
                "entity=%s "
                "study_partition_validation_failed "
                "requested_study=%s "
                "dataframe_studies=%s"
            ),
            entity_name,
            study_id,
            dataframe_studies,
        )

        raise DataValidationError(
            (
                "Multiple studies found in "
                f"entity={entity_name} DataFrame. "
                f"Values={dataframe_studies}"
            )
        )


    actual_study_id = (
        dataframe_studies[0]
    )


    # --------------------------------------------------------
    # Requested study must match DataFrame study
    # --------------------------------------------------------

    if actual_study_id != study_id:

        logger.error(
            (
                "entity=%s "
                "study_partition_mismatch "
                "requested_study=%s "
                "actual_study=%s"
            ),
            entity_name,
            study_id,
            actual_study_id,
        )

        raise DataValidationError(
            (
                "Study partition mismatch. "
                f"requested={study_id}, "
                f"actual={actual_study_id}"
            )
        )


# ============================================================
# BUILD S3 KEY
# ============================================================

def _build_s3_key(
    entity_name: str,
    study_id: str,
    load_date: str,
    run_id: str,
) -> str:
    """
    Build deterministic S3 path.

    Example:

        act/raw/
        study_id=ONC101/
        adverse_event/
        load_date=2026-08-16/
        run_id=manual_test_001/
        adverse_event.csv
    """

    config = _validate_entity(
        entity_name
    )

    safe_study_id = (
        _sanitize_study_id(
            study_id
        )
    )

    safe_run_id = (
        _sanitize_run_id(
            run_id
        )
    )

    valid_load_date = (
        _validate_load_date(
            load_date
        )
    )

    entity_prefix = (
        config["s3_prefix"]
    )


    return (
        "act/raw/"
        f"study_id={safe_study_id}/"
        f"{entity_prefix}/"
        f"load_date={valid_load_date}/"
        f"run_id={safe_run_id}/"
        f"{entity_prefix}.csv"
    )


# ============================================================
# DATAFRAME -> CSV
# ============================================================

def _dataframe_to_csv(
    df: pd.DataFrame,
) -> bytes:
    """
    Convert Pandas DataFrame directly into
    UTF-8 CSV bytes in memory.

    No temporary local file.
    """

    buffer = io.StringIO()

    df.to_csv(
        buffer,
        index=False,
        lineterminator="\n",
    )

    csv_text = (
        buffer.getvalue()
    )

    return csv_text.encode(
        "utf-8"
    )


# ============================================================
# CHECKSUM
# ============================================================

def _calculate_checksum(
    csv_bytes: bytes,
) -> str:
    """
    Calculate SHA-256 checksum.
    """

    return hashlib.sha256(
        csv_bytes
    ).hexdigest()


# ============================================================
# S3 CLIENT
# ============================================================

class ACTS3Client:
    """
    ACT S3 RAW landing client.

    Responsibilities:

    - study-level partitioning
    - entity partitioning
    - run-level idempotency
    - DataFrame -> CSV in memory
    - S3 upload
    - object verification
    - SHA-256 checksum
    - structured logging
    """

    def __init__(
        self,
    ):

        _validate_configuration()


        self.bucket_name = (
            S3_BUCKET_NAME
        )

        self.region = (
            AWS_REGION
        )


        try:

            self.s3_client = boto3.client(
                "s3",
                region_name=self.region,
            )


            logger.info(
                (
                    "s3_client_initialized "
                    "region=%s "
                    "bucket=%s"
                ),
                self.region,
                self.bucket_name,
            )


        except Exception as exc:

            logger.exception(
                "s3_client_initialization_failed"
            )

            raise S3UploadError(
                "Unable to initialize S3 client"
            ) from exc


    # ========================================================
    # VERIFY OBJECT
    # ========================================================

    def _verify_upload(
        self,
        s3_key: str,
        expected_size: int,
        expected_checksum: str,
    ) -> None:
        """
        Verify uploaded object using HEAD.

        Checks:

        - object exists
        - file size
        - SHA-256 metadata
        """

        try:

            response = (
                self.s3_client.head_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                )
            )


        except (
            ClientError,
            BotoCoreError,
        ) as exc:

            logger.exception(
                (
                    "s3_verification_failed "
                    "bucket=%s "
                    "key=%s"
                ),
                self.bucket_name,
                s3_key,
            )

            raise S3ValidationError(
                (
                    "Unable to verify S3 object "
                    f"s3://{self.bucket_name}/{s3_key}"
                )
            ) from exc


        # ----------------------------------------------------
        # FILE SIZE
        # ----------------------------------------------------

        actual_size = (
            response.get(
                "ContentLength",
                -1,
            )
        )


        if actual_size != expected_size:

            raise S3ValidationError(
                (
                    "S3 file size mismatch. "
                    f"expected={expected_size}, "
                    f"actual={actual_size}"
                )
            )


        # ----------------------------------------------------
        # METADATA
        # ----------------------------------------------------

        metadata = (
            response.get(
                "Metadata",
                {},
            )
        )


        actual_checksum = (
            metadata.get(
                "sha256"
            )
        )


        # ----------------------------------------------------
        # CHECKSUM
        # ----------------------------------------------------

        if actual_checksum != expected_checksum:

            raise S3ValidationError(
                (
                    "S3 checksum mismatch. "
                    f"expected={expected_checksum}, "
                    f"actual={actual_checksum}"
                )
            )


        logger.info(
            (
                "s3_upload_verified "
                "bucket=%s "
                "key=%s "
                "size_bytes=%s"
            ),
            self.bucket_name,
            s3_key,
            actual_size,
        )


    # ========================================================
    # UPLOAD
    # ========================================================

    def upload_dataframe(
        self,
        entity_name: str,
        study_id: str,
        df: pd.DataFrame,
        run_id: str,
        load_date: str,
    ) -> S3UploadResult:
        """
        Upload one study + one entity DataFrame.

        Important:

        One call must contain data for only ONE study.

        Example:

            study_id = ONC101
            entity   = adverse_event
        """

        _validate_entity(
            entity_name
        )

        _sanitize_study_id(
            study_id
        )

        _validate_load_date(
            load_date
        )


        logger.info(
            (
                "entity=%s "
                "study_id=%s "
                "s3_upload_started "
                "record_count=%s "
                "run_id=%s "
                "load_date=%s"
            ),
            entity_name,
            study_id,
            len(df),
            run_id,
            load_date,
        )


        # ====================================================
        # EMPTY INCREMENTAL BATCH
        # ====================================================

        if df.empty:

            logger.info(
                (
                    "entity=%s "
                    "study_id=%s "
                    "s3_upload_skipped "
                    "reason=no_records"
                ),
                entity_name,
                study_id,
            )


            return S3UploadResult(

                entity_name=
                    entity_name,

                study_id=
                    study_id,

                uploaded=
                    False,

                record_count=
                    0,

                bucket_name=
                    self.bucket_name,

                s3_key=
                    None,

                s3_uri=
                    None,

                checksum=
                    None,

                file_size_bytes=
                    0,

                run_id=
                    run_id,

                load_date=
                    load_date,
            )


        # ====================================================
        # STUDY SAFETY CHECK
        # ====================================================

        _validate_study_partition(
            entity_name=entity_name,
            study_id=study_id,
            df=df,
        )


        # ====================================================
        # CREATE CSV
        # ====================================================

        try:

            csv_bytes = (
                _dataframe_to_csv(
                    df
                )
            )


        except Exception as exc:

            logger.exception(
                (
                    "entity=%s "
                    "study_id=%s "
                    "dataframe_to_csv_failed"
                ),
                entity_name,
                study_id,
            )


            raise S3UploadError(
                (
                    "Unable to convert DataFrame "
                    f"to CSV entity={entity_name} "
                    f"study_id={study_id}"
                )
            ) from exc


        # ====================================================
        # SIZE + CHECKSUM
        # ====================================================

        file_size = len(
            csv_bytes
        )

        checksum = (
            _calculate_checksum(
                csv_bytes
            )
        )


        # ====================================================
        # BUILD OBJECT KEY
        # ====================================================

        s3_key = (
            _build_s3_key(
                entity_name=entity_name,
                study_id=study_id,
                load_date=load_date,
                run_id=run_id,
            )
        )


        s3_uri = (
            f"s3://"
            f"{self.bucket_name}/"
            f"{s3_key}"
        )


        logger.info(
            (
                "entity=%s "
                "study_id=%s "
                "s3_object_prepared "
                "s3_uri=%s "
                "records=%s "
                "size_bytes=%s "
                "checksum=%s"
            ),
            entity_name,
            study_id,
            s3_uri,
            len(df),
            file_size,
            checksum,
        )


        # ====================================================
        # PUT OBJECT
        # ====================================================

        try:

            self.s3_client.put_object(

                Bucket=
                    self.bucket_name,

                Key=
                    s3_key,

                Body=
                    csv_bytes,

                ContentType=
                    "text/csv",

                Metadata={

                    "study_id":
                        study_id,

                    "entity":
                        entity_name,

                    "run_id":
                        run_id,

                    "record_count":
                        str(
                            len(df)
                        ),

                    "sha256":
                        checksum,
                },
            )


        except (
            ClientError,
            BotoCoreError,
        ) as exc:

            logger.exception(
                (
                    "entity=%s "
                    "study_id=%s "
                    "s3_upload_failed "
                    "bucket=%s "
                    "key=%s"
                ),
                entity_name,
                study_id,
                self.bucket_name,
                s3_key,
            )


            raise S3UploadError(
                (
                    "S3 upload failed "
                    f"entity={entity_name} "
                    f"study_id={study_id}"
                )
            ) from exc


        # ====================================================
        # VERIFY
        # ====================================================

        self._verify_upload(

            s3_key=
                s3_key,

            expected_size=
                file_size,

            expected_checksum=
                checksum,
        )


        # ====================================================
        # SUCCESS
        # ====================================================

        logger.info(
            (
                "entity=%s "
                "study_id=%s "
                "s3_upload_completed "
                "records=%s "
                "s3_uri=%s"
            ),
            entity_name,
            study_id,
            len(df),
            s3_uri,
        )


        return S3UploadResult(

            entity_name=
                entity_name,

            study_id=
                study_id,

            uploaded=
                True,

            record_count=
                len(df),

            bucket_name=
                self.bucket_name,

            s3_key=
                s3_key,

            s3_uri=
                s3_uri,

            checksum=
                checksum,

            file_size_bytes=
                file_size,

            run_id=
                run_id,

            load_date=
                load_date,
        )