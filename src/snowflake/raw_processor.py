# src/snowflake/raw_processor.py

from __future__ import annotations

from dataclasses import (
    asdict,
    dataclass,
)

import os

from pathlib import Path

from typing import Any


import snowflake.connector


from src.common.exceptions import (
    ConfigurationError,
)

from src.common.logging_config import (
    get_logger,
)


logger = get_logger(__name__)


# ============================================================
# PROJECT / SQL PATHS
# ============================================================

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[2]
)

SNOWFLAKE_SQL_DIR = (
    PROJECT_ROOT
    / "snowflake"
    / "sql"
)


# ============================================================
# SNOWFLAKE CONNECTION
# ============================================================

DEFAULT_CONNECTION_NAME = (
    "SNOWFLAKE_ACT_DEV"
)


# ============================================================
# ENTITY -> EXISTING RAW PROCESS SQL
# ============================================================
#
# These are the already-tested Option 3 SQL files.
#
# Each file performs:
#
#     S3
#      ↓
#     LND_<ENTITY>
#      ↓
#     RAW_<ENTITY>_HISTORY
#      ↓
#     RAW_<ENTITY>_CURRENT
#
# COPY uses FORCE = FALSE inside the SQL, so Snowflake's
# load history protects normal replay of the same file.
# ============================================================

RAW_PROCESS_SQL_FILES = {
    "study":
        "012_process_study_option3.sql",

    "site":
        "014_process_site_option3.sql",

    "subject":
        "016_process_subject_option3.sql",

    "visit":
        "018_process_visit_option3.sql",

    "adverse_event":
        "010_process_adverse_event_option3.sql",

    "lab_result":
        "020_process_lab_result_option3.sql",

    "protocol_deviation":
        "023_process_protocol_deviation_option3.sql",

    "data_query":
        "026_process_data_query_option3.sql",
}


# ============================================================
# DEFAULT PROCESSING ORDER
# ============================================================
#
# This order is useful for manual --all execution.
#
# The RAW tables themselves are processed independently.
# Later Airflow can map one Snowflake task per entity.
# ============================================================

RAW_PROCESS_ORDER = [
    "study",
    "site",
    "subject",
    "visit",
    "adverse_event",
    "lab_result",
    "protocol_deviation",
    "data_query",
]


# ============================================================
# RESULT
# ============================================================

@dataclass
class RawProcessResult:
    """
    Small metadata object returned after processing one
    Snowflake RAW entity.

    Keep this metadata small so it can later be returned
    through Airflow XCom safely.
    """

    entity_name: str

    sql_file: str

    statements_executed: int

    query_ids: list[str]

    status: str


    def to_dict(
        self,
    ) -> dict[str, Any]:
        """
        Return a JSON/XCom-friendly dictionary.
        """

        return asdict(
            self
        )


# ============================================================
# CONNECTION NAME
# ============================================================

def _get_connection_name() -> str:
    """
    Resolve the named Snowflake connection.

    Default:
        SNOWFLAKE_ACT_DEV

    Optional override:
        SNOWFLAKE_CONNECTION_NAME
    """

    value = os.getenv(
        "SNOWFLAKE_CONNECTION_NAME",
        DEFAULT_CONNECTION_NAME,
    ).strip()


    if not value:

        raise ConfigurationError(
            "SNOWFLAKE_CONNECTION_NAME is empty"
        )


    return value


# ============================================================
# ENTITY VALIDATION
# ============================================================

def _validate_entity(
    entity_name: str,
) -> str:
    """
    Validate and normalize RAW entity name.
    """

    if not entity_name:

        raise ConfigurationError(
            "entity_name cannot be empty"
        )


    cleaned = (
        entity_name
        .strip()
        .lower()
    )


    if cleaned not in RAW_PROCESS_SQL_FILES:

        raise ConfigurationError(
            (
                f"Unknown RAW entity={cleaned}. "
                f"Supported entities="
                f"{sorted(RAW_PROCESS_SQL_FILES)}"
            )
        )


    return cleaned


# ============================================================
# SQL FILE RESOLUTION
# ============================================================

def get_raw_process_sql_path(
    entity_name: str,
) -> Path:
    """
    Resolve the existing Option 3 SQL file for one entity.
    """

    cleaned = (
        _validate_entity(
            entity_name
        )
    )


    sql_path = (
        SNOWFLAKE_SQL_DIR
        / RAW_PROCESS_SQL_FILES[
            cleaned
        ]
    )


    if not sql_path.is_file():

        raise ConfigurationError(
            (
                "Snowflake RAW process SQL file "
                "was not found "
                f"entity={cleaned} "
                f"path={sql_path}"
            )
        )


    return sql_path


# ============================================================
# RAW PROCESSOR
# ============================================================

class SnowflakeRawProcessor:
    """
    Execute ACT's existing multi-statement RAW SQL files
    using the Snowflake Python Connector.

    This class does NOT rebuild the SQL logic in Python.

    Snowflake remains responsible for:

        COPY INTO
        hash calculation
        validation queries
        HISTORY MERGE
        CURRENT MERGE
        landing processed flag

    Python only orchestrates the existing SQL file.
    """

    def __init__(
        self,
        connection_name: str | None = None,
    ) -> None:

        self.connection_name = (
            connection_name
            or _get_connection_name()
        )


    # ========================================================
    # CONNECTION
    # ========================================================

    def _connect(
        self,
        entity_name: str,
    ):
        """
        Open a short-lived Snowflake connection.

        Autocommit is intentionally left at the connector /
        Snowflake default because the existing SQL files were
        already designed and tested as independently rerunnable
        statements.

        Their replay protection comes from:
            COPY FORCE = FALSE
            HISTORY MERGE uniqueness
            CURRENT deterministic MERGE
        """

        try:

            return snowflake.connector.connect(
                connection_name=
                    self.connection_name,

                application=(
                    "ACT_DATA_PLATFORM_RAW_"
                    f"{entity_name.upper()}"
                ),
            )


        except Exception as exc:

            logger.exception(
                (
                    "snowflake_raw_connection_failed "
                    "entity=%s "
                    "connection_name=%s"
                ),
                entity_name,
                self.connection_name,
            )


            raise RuntimeError(
                (
                    "Unable to connect to Snowflake "
                    f"for RAW entity={entity_name}"
                )
            ) from exc


    # ========================================================
    # PROCESS ONE ENTITY
    # ========================================================

    def process_entity(
        self,
        entity_name: str,
    ) -> RawProcessResult:
        """
        Execute the existing RAW process SQL for one entity.

        Example:

            adverse_event

        executes:

            snowflake/sql/
            010_process_adverse_event_option3.sql
        """

        cleaned_entity = (
            _validate_entity(
                entity_name
            )
        )


        sql_path = (
            get_raw_process_sql_path(
                cleaned_entity
            )
        )


        logger.info(
            (
                "snowflake_raw_processing_started "
                "entity=%s "
                "sql_file=%s"
            ),
            cleaned_entity,
            sql_path.name,
        )


        conn = None

        query_ids: list[str] = []

        statements_executed = 0


        try:

            conn = self._connect(
                cleaned_entity
            )


            # =================================================
            # EXECUTE COMPLETE SQL SCRIPT
            # =================================================
            #
            # remove_comments=True is intentional because the
            # ACT SQL files contain many comments.
            # =================================================

            with sql_path.open(
                "r",
                encoding="utf-8",
            ) as sql_stream:


                for result_cursor in (
                    conn.execute_stream(
                        sql_stream,
                        remove_comments=True,
                    )
                ):

                    statements_executed += 1


                    query_id = getattr(
                        result_cursor,
                        "sfqid",
                        None,
                    )


                    if query_id:

                        query_ids.append(
                            query_id
                        )


                    logger.info(
                        (
                            "snowflake_raw_statement_completed "
                            "entity=%s "
                            "statement_number=%s "
                            "query_id=%s "
                            "rowcount=%s"
                        ),
                        cleaned_entity,
                        statements_executed,
                        query_id,
                        getattr(
                            result_cursor,
                            "rowcount",
                            None,
                        ),
                    )


            logger.info(
                (
                    "snowflake_raw_processing_completed "
                    "entity=%s "
                    "status=SUCCESS "
                    "sql_file=%s "
                    "statements=%s"
                ),
                cleaned_entity,
                sql_path.name,
                statements_executed,
            )


            return RawProcessResult(

                entity_name=
                    cleaned_entity,

                sql_file=
                    sql_path.name,

                statements_executed=
                    statements_executed,

                query_ids=
                    query_ids,

                status=
                    "SUCCESS",
            )


        except Exception as exc:

            logger.exception(
                (
                    "snowflake_raw_processing_failed "
                    "entity=%s "
                    "sql_file=%s "
                    "statements_completed=%s"
                ),
                cleaned_entity,
                sql_path.name,
                statements_executed,
            )


            raise RuntimeError(
                (
                    "Snowflake RAW processing failed "
                    f"entity={cleaned_entity} "
                    f"sql_file={sql_path.name}"
                )
            ) from exc


        finally:

            if conn is not None:

                conn.close()
