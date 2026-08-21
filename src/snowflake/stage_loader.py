# src/snowflake/stage_loader.py

from __future__ import annotations

from dataclasses import asdict, dataclass
import os
import re
from pathlib import Path
from typing import Any

import snowflake.connector

from config.endpoints import ENDPOINTS

from src.common.exceptions import ConfigurationError
from src.common.logging_config import get_logger


logger = get_logger(__name__)


# ============================================================
# DEFAULT CONFIGURATION
# ============================================================

DEFAULT_CONNECTION_NAME = "SNOWFLAKE_ACT_DEV"

DEFAULT_RAW_STAGE = "ACT_DB.RAW.ACT_RAW_STAGE"


# ============================================================
# RESULT
# ============================================================

@dataclass
class StageUploadResult:
    """
    Metadata returned after uploading one local RAW
    file to the Snowflake internal stage.
    """

    entity_name: str
    study_id: str

    local_file_path: str

    stage_name: str
    stage_prefix: str
    stage_uri: str

    source_file_name: str
    target_file_name: str | None

    source_size_bytes: int
    target_size_bytes: int | None

    upload_status: str

    query_id: str | None


    def to_dict(self) -> dict[str, Any]:
        """
        Return Airflow XCom / JSON friendly metadata.
        """

        return asdict(self)


# ============================================================
# CONNECTION NAME
# ============================================================

def _get_connection_name() -> str:
    """
    Return the configured Snowflake named connection.

    Default:

        SNOWFLAKE_ACT_DEV
    """

    connection_name = os.getenv(
        "SNOWFLAKE_CONNECTION_NAME",
        DEFAULT_CONNECTION_NAME,
    ).strip()


    if not connection_name:

        raise ConfigurationError(
            "SNOWFLAKE_CONNECTION_NAME cannot be empty"
        )


    return connection_name


# ============================================================
# RAW STAGE NAME
# ============================================================

def _get_stage_name() -> str:
    """
    Return the Snowflake RAW internal stage.

    Default:

        ACT_DB.RAW.ACT_RAW_STAGE
    """

    stage_name = os.getenv(
        "SNOWFLAKE_RAW_STAGE",
        DEFAULT_RAW_STAGE,
    ).strip()


    if not stage_name:

        raise ConfigurationError(
            "SNOWFLAKE_RAW_STAGE cannot be empty"
        )


    # Only allow:
    #
    # DATABASE.SCHEMA.STAGE
    #
    # Prevent arbitrary SQL from entering a PUT statement.

    if not re.fullmatch(
        r"[A-Za-z0-9_$]+\.[A-Za-z0-9_$]+\.[A-Za-z0-9_$]+",
        stage_name,
    ):

        raise ConfigurationError(
            (
                "SNOWFLAKE_RAW_STAGE must use "
                "DATABASE.SCHEMA.STAGE format. "
                f"Received={stage_name}"
            )
        )


    return stage_name


# ============================================================
# SAFE PATH COMPONENT
# ============================================================

def _sanitize_path_component(
    value: str,
    field_name: str,
) -> str:
    """
    Convert a value to a safe stage-path component.

    Example:

        manual__2026-08-20T10:00:00+00:00

    becomes:

        manual__2026-08-20T10-00-00+00-00
    """

    if not value:

        raise ConfigurationError(
            f"{field_name} cannot be empty"
        )


    cleaned = re.sub(
        r"[^A-Za-z0-9_.+\-=]",
        "-",
        value.strip(),
    )


    if not cleaned:

        raise ConfigurationError(
            (
                f"{field_name} became empty "
                "after sanitization"
            )
        )


    return cleaned


# ============================================================
# STORAGE PREFIX
# ============================================================

def _get_storage_prefix(
    entity_name: str,
) -> str:
    """
    Return generic storage prefix for an ACT entity.
    """

    cleaned_entity = (
        entity_name
        .strip()
        .lower()
    )


    if cleaned_entity not in ENDPOINTS:

        raise ConfigurationError(
            f"Unknown entity={cleaned_entity}"
        )


    storage_prefix = (
        ENDPOINTS[
            cleaned_entity
        ].get(
            "storage_prefix"
        )
    )


    if not storage_prefix:

        raise ConfigurationError(
            (
                "storage_prefix missing "
                f"for entity={cleaned_entity}"
            )
        )


    return _sanitize_path_component(
        str(storage_prefix),
        "storage_prefix",
    )


# ============================================================
# STAGE PREFIX
# ============================================================

def _build_stage_prefix(
    *,
    entity_name: str,
    study_id: str,
    load_date: str,
    run_id: str,
) -> str:
    """
    Build the logical Snowflake stage partition.

    Example:

        study_id=ONC101/
        study/
        load_date=2026-08-20/
        run_id=local_test_001
    """

    clean_study_id = (
        _sanitize_path_component(
            study_id,
            "study_id",
        )
    )


    clean_load_date = (
        _sanitize_path_component(
            load_date,
            "load_date",
        )
    )


    clean_run_id = (
        _sanitize_path_component(
            run_id,
            "run_id",
        )
    )


    storage_prefix = (
        _get_storage_prefix(
            entity_name
        )
    )


    return (
        f"study_id={clean_study_id}/"
        f"{storage_prefix}/"
        f"load_date={clean_load_date}/"
        f"run_id={clean_run_id}"
    )


# ============================================================
# LOCAL FILE VALIDATION
# ============================================================

def _validate_local_file(
    *,
    local_file_path: str | Path,
    entity_name: str,
) -> Path:
    """
    Validate that the expected local RAW CSV exists.
    """

    file_path = (
        Path(local_file_path)
        .expanduser()
        .resolve()
    )


    if not file_path.is_file():

        raise FileNotFoundError(
            (
                "Local RAW file does not exist. "
                f"path={file_path}"
            )
        )


    storage_prefix = (
        _get_storage_prefix(
            entity_name
        )
    )


    expected_file_name = (
        f"{storage_prefix}.csv"
    )


    if file_path.name != expected_file_name:

        raise ConfigurationError(
            (
                "Unexpected RAW filename. "
                f"entity={entity_name} "
                f"expected={expected_file_name} "
                f"actual={file_path.name}"
            )
        )


    return file_path


# ============================================================
# SNOWFLAKE LOCAL FILE URI
# ============================================================

def _build_local_file_uri(
    file_path: Path,
) -> str:
    """
    Build the local file URI used by Snowflake PUT.

    IMPORTANT
    ---------

    Do not use:

        Path.as_uri()

    for ACT partition paths.

    Python URL-encodes characters such as:

        =  ->  %3D

    Example:

        study_id=ONC101

    becomes:

        study_id%3DONC101

    Snowflake PUT then searches the physical filesystem
    for the encoded path and fails with:

        File doesn't exist

    Because the ACT process runs inside WSL/Linux, the
    correct Snowflake PUT URI is built directly from the
    POSIX filesystem path.

    Example:

        /mnt/c/Code/.../study_id=ONC101/study.csv

    becomes:

        file:///mnt/c/Code/.../study_id=ONC101/study.csv
    """

    resolved_path = (
        file_path
        .expanduser()
        .resolve()
    )


    path_text = (
        resolved_path.as_posix()
    )


    if "'" in path_text:

        raise ConfigurationError(
            (
                "Local RAW path cannot contain "
                "a single quote because it is used "
                "inside a Snowflake PUT statement. "
                f"path={resolved_path}"
            )
        )


    return (
        f"file://{path_text}"
    )


# ============================================================
# SNOWFLAKE STAGE LOADER
# ============================================================

class SnowflakeStageLoader:
    """
    Upload local ACT RAW files into the Snowflake
    internal RAW stage.

    Flow:

        LocalStorageBackend
                |
                v
          data/raw/*.csv
                |
                | PUT
                v
        @ACT_RAW_STAGE

    This class performs staging only.

    It does not perform:

        COPY INTO
        RAW HISTORY load
        RAW CURRENT merge
    """


    def __init__(
        self,
        connection_name: str | None = None,
        stage_name: str | None = None,
    ) -> None:

        self.connection_name = (
            connection_name
            or _get_connection_name()
        )


        self.stage_name = (
            stage_name
            or _get_stage_name()
        )


    # ========================================================
    # CONNECTION
    # ========================================================

    def _connect(self):
        """
        Create a short-lived Snowflake connection.
        """

        try:

            return snowflake.connector.connect(
                connection_name=
                    self.connection_name,

                application=
                    "ACT_DATA_PLATFORM_STAGE_LOADER",
            )


        except Exception as exc:

            logger.exception(
                (
                    "snowflake_stage_connection_failed "
                    "connection_name=%s"
                ),
                self.connection_name,
            )


            raise RuntimeError(
                (
                    "Unable to connect to Snowflake "
                    "for internal-stage upload"
                )
            ) from exc


    # ========================================================
    # UPLOAD FILE
    # ========================================================

    def upload_file(
        self,
        *,
        local_file_path: str | Path,
        entity_name: str,
        study_id: str,
        load_date: str,
        run_id: str,
    ) -> StageUploadResult:
        """
        Upload one local RAW CSV into ACT_RAW_STAGE.
        """

        cleaned_entity = (
            entity_name
            .strip()
            .lower()
        )


        # ====================================================
        # VALIDATE LOCAL FILE
        # ====================================================

        file_path = (
            _validate_local_file(
                local_file_path=
                    local_file_path,

                entity_name=
                    cleaned_entity,
            )
        )


        # ====================================================
        # BUILD STAGE PARTITION
        # ====================================================

        stage_prefix = (
            _build_stage_prefix(
                entity_name=
                    cleaned_entity,

                study_id=
                    study_id,

                load_date=
                    load_date,

                run_id=
                    run_id,
            )
        )


        stage_directory_uri = (
            f"@{self.stage_name}/"
            f"{stage_prefix}"
        )


        staged_file_uri = (
            f"{stage_directory_uri}/"
            f"{file_path.name}"
        )


        # ====================================================
        # BUILD LOCAL PUT URI
        # ====================================================

        local_file_uri = (
            _build_local_file_uri(
                file_path
            )
        )


        source_size_bytes = (
            file_path.stat().st_size
        )


        logger.info(
            (
                "snowflake_stage_upload_started "
                "entity=%s "
                "study_id=%s "
                "local_file=%s "
                "stage_uri=%s "
                "size_bytes=%s"
            ),
            cleaned_entity,
            study_id,
            file_path,
            stage_directory_uri,
            source_size_bytes,
        )


        conn = None
        cursor = None


        try:

            conn = (
                self._connect()
            )


            cursor = (
                conn.cursor()
            )


            # =================================================
            # PUT
            # =================================================
            #
            # AUTO_COMPRESS = FALSE
            #
            # Keeps:
            #
            #     study.csv
            #
            # instead of:
            #
            #     study.csv.gz
            #
            #
            # OVERWRITE = FALSE
            #
            # The run_id partition provides a deterministic
            # retry location and avoids accidental replacement.
            # =================================================

            put_sql = (
                f"PUT '{local_file_uri}' "
                f"{stage_directory_uri} "
                "AUTO_COMPRESS = FALSE "
                "OVERWRITE = FALSE"
            )


            logger.info(
                (
                    "snowflake_put_started "
                    "entity=%s "
                    "study_id=%s "
                    "stage_uri=%s"
                ),
                cleaned_entity,
                study_id,
                stage_directory_uri,
            )


            cursor.execute(
                put_sql
            )


            query_id = getattr(
                cursor,
                "sfqid",
                None,
            )


            put_rows = (
                cursor.fetchall()
            )


            if not put_rows:

                raise RuntimeError(
                    (
                        "Snowflake PUT returned "
                        "no result rows"
                    )
                )


            row = (
                put_rows[0]
            )


            # =================================================
            # PUT RESULT COLUMNS
            # =================================================
            #
            # Typical Snowflake result:
            #
            # source
            # target
            # source_size
            # target_size
            # source_compression
            # target_compression
            # status
            # message
            # =================================================

            source_name = (
                str(row[0])
                if len(row) > 0
                and row[0] is not None
                else file_path.name
            )


            target_name = (
                str(row[1])
                if len(row) > 1
                and row[1] is not None
                else None
            )


            target_size = (
                int(row[3])
                if len(row) > 3
                and row[3] is not None
                else None
            )


            upload_status = (
                str(row[6])
                if len(row) > 6
                and row[6] is not None
                else "UNKNOWN"
            )


            normalized_status = (
                upload_status
                .strip()
                .upper()
            )


            accepted_statuses = {
                "UPLOADED",
                "SKIPPED",
            }


            if (
                normalized_status
                not in accepted_statuses
            ):

                message = (
                    str(row[7])
                    if len(row) > 7
                    and row[7] is not None
                    else None
                )


                raise RuntimeError(
                    (
                        "Snowflake PUT failed. "
                        f"status={normalized_status} "
                        f"message={message}"
                    )
                )


            # =================================================
            # VERIFY STAGED FILE
            # ====================================================

            cursor.execute(
                f"LIST {stage_directory_uri}"
            )


            staged_rows = (
                cursor.fetchall()
            )


            expected_file_name = (
                file_path.name
            )


            matching_rows = [
                staged_row

                for staged_row in staged_rows

                if (
                    len(staged_row) > 0
                    and str(
                        staged_row[0]
                    ).endswith(
                        f"/{expected_file_name}"
                    )
                )
            ]


            if not matching_rows:

                raise RuntimeError(
                    (
                        "Snowflake stage verification failed. "
                        f"Expected file={expected_file_name} "
                        f"stage={stage_directory_uri}"
                    )
                )


            logger.info(
                (
                    "snowflake_stage_upload_completed "
                    "entity=%s "
                    "study_id=%s "
                    "status=%s "
                    "staged_file=%s "
                    "query_id=%s"
                ),
                cleaned_entity,
                study_id,
                normalized_status,
                staged_file_uri,
                query_id,
            )


            return StageUploadResult(
                entity_name=
                    cleaned_entity,

                study_id=
                    study_id,

                local_file_path=
                    str(file_path),

                stage_name=
                    self.stage_name,

                stage_prefix=
                    stage_prefix,

                stage_uri=
                    staged_file_uri,

                source_file_name=
                    source_name,

                target_file_name=
                    target_name,

                source_size_bytes=
                    source_size_bytes,

                target_size_bytes=
                    target_size,

                upload_status=
                    normalized_status,

                query_id=
                    query_id,
            )


        except Exception:

            logger.exception(
                (
                    "snowflake_stage_upload_failed "
                    "entity=%s "
                    "study_id=%s "
                    "local_file=%s "
                    "stage_uri=%s"
                ),
                cleaned_entity,
                study_id,
                file_path,
                stage_directory_uri,
            )


            raise


        finally:

            if cursor is not None:

                cursor.close()


            if conn is not None:

                conn.close()