# src/storage/local_storage.py

import hashlib
import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

from config.endpoints import ENDPOINTS

from src.common.exceptions import (
    ConfigurationError,
    DataValidationError,
    StorageValidationError,
    StorageWriteError,
)

from src.common.logging_config import get_logger

from src.storage.storage_backend import (
    StorageBackend,
    StorageWriteResult,
)


logger = get_logger(__name__)


# ============================================================
# PROJECT ROOT
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]


# ============================================================
# LOCAL STORAGE ROOT
# ============================================================

DEFAULT_LOCAL_STORAGE_ROOT = (
    PROJECT_ROOT
    / "data"
    / "raw"
)


def _get_storage_root() -> Path:
    """
    Resolve the local RAW storage directory.

    Default:

        <project>/data/raw

    Optional environment override:

        LOCAL_STORAGE_ROOT=/some/path

    Relative paths are resolved from the project root,
    not from the current terminal working directory.
    """

    configured_root = os.getenv(
        "LOCAL_STORAGE_ROOT"
    )

    if not configured_root:

        return DEFAULT_LOCAL_STORAGE_ROOT.resolve()


    configured_path = Path(
        configured_root
    ).expanduser()


    if configured_path.is_absolute():

        return configured_path.resolve()


    return (
        PROJECT_ROOT
        / configured_path
    ).resolve()


# ============================================================
# ENTITY VALIDATION
# ============================================================

def _validate_entity(
    entity_name: str,
) -> dict:
    """
    Verify the requested entity exists in ENDPOINTS.
    """

    if not entity_name:

        raise ConfigurationError(
            "entity_name cannot be empty"
        )


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
    Make study_id safe for a filesystem path.
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
    Convert Airflow run_id into a filesystem-safe value.

    Example:

        scheduled__2026-08-20T10:00:00+00:00

    becomes:

        scheduled__2026-08-20T10-00-00+00-00
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


    if not cleaned:

        raise ConfigurationError(
            (
                "run_id became empty "
                "after sanitization"
            )
        )


    return cleaned


# ============================================================
# LOAD DATE VALIDATION
# ============================================================

def _validate_load_date(
    load_date: str,
) -> str:
    """
    Ensure load_date uses YYYY-MM-DD.
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
# STORAGE PREFIX
# ============================================================

def _get_storage_prefix(
    entity_name: str,
) -> str:
    """
    Return the logical storage folder for an entity.

    During migration the existing config still contains:

        s3_prefix

    Later we will rename it to:

        storage_prefix

    The fallback keeps this file compatible while we
    migrate the repository one file at a time.
    """

    config = _validate_entity(
        entity_name
    )


    prefix = (
        config.get(
            "storage_prefix"
        )
        or config.get(
            "s3_prefix"
        )
    )


    if not prefix:

        raise ConfigurationError(
            (
                "Storage prefix missing "
                f"for entity={entity_name}"
            )
        )


    cleaned = re.sub(
        r"[^A-Za-z0-9_.\-]",
        "-",
        str(prefix).strip(),
    )


    if not cleaned:

        raise ConfigurationError(
            (
                "Storage prefix became empty "
                f"for entity={entity_name}"
            )
        )


    return cleaned


# ============================================================
# STUDY PARTITION VALIDATION
# ============================================================

def _validate_study_partition(
    entity_name: str,
    study_id: str,
    dataframe: pd.DataFrame,
) -> None:
    """
    Protect against writing records from another study
    into the requested study partition.

    Example:

        requested partition:

            ONC101

        DataFrame contains:

            ONC101
            ONC102

        The write must fail.
    """

    if dataframe.empty:

        return


    if "study_id" not in dataframe.columns:

        raise DataValidationError(
            (
                "study_id column missing "
                f"for entity={entity_name}"
            )
        )


    if dataframe[
        "study_id"
    ].isna().any():

        raise DataValidationError(
            (
                "NULL study_id found "
                f"for entity={entity_name}"
            )
        )


    dataframe_studies = (
        dataframe[
            "study_id"
        ]
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )


    if len(
        dataframe_studies
    ) != 1:

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
# BUILD LOCAL FILE PATH
# ============================================================

def _build_local_path(
    storage_root: Path,
    entity_name: str,
    study_id: str,
    load_date: str,
    run_id: str,
) -> Path:
    """
    Build deterministic local RAW file path.

    Example:

        data/raw/
        study_id=ONC101/
        adverse_event/
        load_date=2026-08-20/
        run_id=manual_test_001/
        adverse_event.csv
    """

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


    storage_prefix = (
        _get_storage_prefix(
            entity_name
        )
    )


    return (
        storage_root
        / f"study_id={safe_study_id}"
        / storage_prefix
        / f"load_date={valid_load_date}"
        / f"run_id={safe_run_id}"
        / f"{storage_prefix}.csv"
    )


# ============================================================
# DATAFRAME -> CSV BYTES
# ============================================================

def _dataframe_to_csv_bytes(
    dataframe: pd.DataFrame,
) -> bytes:
    """
    Convert DataFrame to deterministic UTF-8 CSV bytes.
    """

    csv_text = dataframe.to_csv(
        index=False,
        lineterminator="\n",
    )


    return csv_text.encode(
        "utf-8"
    )


# ============================================================
# CHECKSUM
# ============================================================

def _calculate_checksum(
    content: bytes,
) -> str:
    """
    Return SHA-256 checksum.
    """

    return hashlib.sha256(
        content
    ).hexdigest()


# ============================================================
# LOCAL STORAGE BACKEND
# ============================================================

class LocalStorageBackend(
    StorageBackend
):
    """
    Local filesystem implementation of ACT storage.

    Responsibilities:

    - entity validation
    - study-level partitioning
    - run-level idempotent path
    - DataFrame -> CSV
    - atomic local write
    - file verification
    - SHA-256 verification
    - structured logging
    """


    def __init__(
        self,
        storage_root: str | Path | None = None,
    ):

        if storage_root is None:

            self.storage_root = (
                _get_storage_root()
            )

        else:

            supplied_root = Path(
                storage_root
            ).expanduser()


            if supplied_root.is_absolute():

                self.storage_root = (
                    supplied_root.resolve()
                )

            else:

                self.storage_root = (
                    PROJECT_ROOT
                    / supplied_root
                ).resolve()


        try:

            self.storage_root.mkdir(
                parents=True,
                exist_ok=True,
            )


        except OSError as exc:

            logger.exception(
                (
                    "local_storage_initialization_failed "
                    "storage_root=%s"
                ),
                self.storage_root,
            )


            raise StorageWriteError(
                (
                    "Unable to create local "
                    "storage directory "
                    f"path={self.storage_root}"
                )
            ) from exc


        logger.info(
            (
                "local_storage_initialized "
                "storage_root=%s"
            ),
            self.storage_root,
        )


    # ========================================================
    # BACKEND NAME
    # ========================================================

    @property
    def backend_name(
        self,
    ) -> str:

        return "local"


    # ========================================================
    # VERIFY FILE
    # ========================================================

    def _verify_file(
        self,
        file_path: Path,
        expected_size: int,
        expected_checksum: str,
    ) -> None:
        """
        Verify:

        - file exists
        - file size matches
        - checksum matches
        """

        if not file_path.is_file():

            raise StorageValidationError(
                (
                    "Local file does not exist "
                    f"path={file_path}"
                )
            )


        actual_size = (
            file_path.stat().st_size
        )


        if actual_size != expected_size:

            raise StorageValidationError(
                (
                    "Local file size mismatch. "
                    f"expected={expected_size}, "
                    f"actual={actual_size}, "
                    f"path={file_path}"
                )
            )


        try:

            actual_bytes = (
                file_path.read_bytes()
            )


        except OSError as exc:

            raise StorageValidationError(
                (
                    "Unable to read local file "
                    f"during verification "
                    f"path={file_path}"
                )
            ) from exc


        actual_checksum = (
            _calculate_checksum(
                actual_bytes
            )
        )


        if actual_checksum != expected_checksum:

            raise StorageValidationError(
                (
                    "Local checksum mismatch. "
                    f"expected={expected_checksum}, "
                    f"actual={actual_checksum}, "
                    f"path={file_path}"
                )
            )


        logger.info(
            (
                "local_storage_verified "
                "path=%s "
                "size_bytes=%s"
            ),
            file_path,
            actual_size,
        )


    # ========================================================
    # WRITE DATAFRAME
    # ========================================================

    def write_dataframe(
        self,
        entity_name: str,
        study_id: str,
        dataframe: pd.DataFrame,
        run_id: str,
        load_date: str,
    ) -> StorageWriteResult:
        """
        Persist one study + one entity DataFrame.

        One invocation may contain records for only
        one study.
        """

        _validate_entity(
            entity_name
        )


        _sanitize_study_id(
            study_id
        )


        _sanitize_run_id(
            run_id
        )


        _validate_load_date(
            load_date
        )


        logger.info(
            (
                "entity=%s "
                "study_id=%s "
                "local_storage_write_started "
                "record_count=%s "
                "run_id=%s "
                "load_date=%s"
            ),
            entity_name,
            study_id,
            len(dataframe),
            run_id,
            load_date,
        )


        # ====================================================
        # EMPTY INCREMENTAL BATCH
        # ====================================================

        if dataframe.empty:

            logger.info(
                (
                    "entity=%s "
                    "study_id=%s "
                    "local_storage_write_skipped "
                    "reason=no_records"
                ),
                entity_name,
                study_id,
            )


            return StorageWriteResult(

                entity_name=
                    entity_name,

                study_id=
                    study_id,

                stored=
                    False,

                record_count=
                    0,

                storage_backend=
                    self.backend_name,

                storage_path=
                    None,

                storage_uri=
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

            entity_name=
                entity_name,

            study_id=
                study_id,

            dataframe=
                dataframe,
        )


        # ====================================================
        # CREATE CSV
        # ====================================================

        try:

            csv_bytes = (
                _dataframe_to_csv_bytes(
                    dataframe
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


            raise StorageWriteError(
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
        # BUILD FILE PATH
        # ====================================================

        file_path = (
            _build_local_path(

                storage_root=
                    self.storage_root,

                entity_name=
                    entity_name,

                study_id=
                    study_id,

                load_date=
                    load_date,

                run_id=
                    run_id,
            )
        )


        storage_uri = (
            file_path.as_uri()
        )


        logger.info(
            (
                "entity=%s "
                "study_id=%s "
                "local_file_prepared "
                "storage_uri=%s "
                "records=%s "
                "size_bytes=%s "
                "checksum=%s"
            ),
            entity_name,
            study_id,
            storage_uri,
            len(dataframe),
            file_size,
            checksum,
        )


        # ====================================================
        # CREATE DIRECTORY
        # ====================================================

        try:

            file_path.parent.mkdir(
                parents=True,
                exist_ok=True,
            )


        except OSError as exc:

            logger.exception(
                (
                    "entity=%s "
                    "study_id=%s "
                    "local_directory_creation_failed "
                    "path=%s"
                ),
                entity_name,
                study_id,
                file_path.parent,
            )


            raise StorageWriteError(
                (
                    "Unable to create local "
                    "storage directory "
                    f"path={file_path.parent}"
                )
            ) from exc


        # ====================================================
        # ATOMIC WRITE
        # ====================================================

        temporary_path = (
            file_path.with_suffix(
                ".csv.tmp"
            )
        )


        try:

            temporary_path.write_bytes(
                csv_bytes
            )


            temporary_path.replace(
                file_path
            )


        except OSError as exc:

            logger.exception(
                (
                    "entity=%s "
                    "study_id=%s "
                    "local_storage_write_failed "
                    "path=%s"
                ),
                entity_name,
                study_id,
                file_path,
            )


            if temporary_path.exists():

                try:

                    temporary_path.unlink()

                except OSError:

                    logger.warning(
                        (
                            "temporary_file_cleanup_failed "
                            "path=%s"
                        ),
                        temporary_path,
                    )


            raise StorageWriteError(
                (
                    "Local storage write failed "
                    f"entity={entity_name} "
                    f"study_id={study_id}"
                )
            ) from exc


        # ====================================================
        # VERIFY
        # ====================================================

        self._verify_file(

            file_path=
                file_path,

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
                "local_storage_write_completed "
                "records=%s "
                "storage_uri=%s"
            ),
            entity_name,
            study_id,
            len(dataframe),
            storage_uri,
        )


        return StorageWriteResult(

            entity_name=
                entity_name,

            study_id=
                study_id,

            stored=
                True,

            record_count=
                len(dataframe),

            storage_backend=
                self.backend_name,

            storage_path=
                str(
                    file_path
                ),

            storage_uri=
                storage_uri,

            checksum=
                checksum,

            file_size_bytes=
                file_size,

            run_id=
                run_id,

            load_date=
                load_date,
        )