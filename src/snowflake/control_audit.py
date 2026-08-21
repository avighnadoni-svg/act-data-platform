"""
ACT CONTROL audit writer.

Writes operational audit records to:

    ACT_DB.CONTROL.PIPELINE_RUN_AUDIT
    ACT_DB.CONTROL.ENTITY_LOAD_AUDIT
    ACT_DB.CONTROL.REPROCESS_AUDIT

The CONTROL layer is storage-neutral.

Examples of storage implementations:

    local filesystem
    Amazon S3
    Azure Data Lake Storage
    Google Cloud Storage

The audit layer records generic storage metadata rather than
cloud-provider-specific metadata.

Connection
----------
By default this module reuses the named Snowflake connection:

    SNOWFLAKE_ACT_DEV

Override it with:

    SNOWFLAKE_CONNECTION_NAME=<connection-name>
"""

from __future__ import annotations

import os
from typing import Any
from uuid import uuid4

import snowflake.connector
from snowflake.connector import SnowflakeConnection

from src.common.logging_config import get_logger


logger = get_logger(__name__)


DEFAULT_CONNECTION_NAME = "SNOWFLAKE_ACT_DEV"


PIPELINE_TABLE = (
    "ACT_DB.CONTROL.PIPELINE_RUN_AUDIT"
)

ENTITY_TABLE = (
    "ACT_DB.CONTROL.ENTITY_LOAD_AUDIT"
)

REPROCESS_TABLE = (
    "ACT_DB.CONTROL.REPROCESS_AUDIT"
)


# ============================================================================
# EXCEPTIONS
# ============================================================================

class ControlAuditError(RuntimeError):
    """
    Raised when writing ACT CONTROL audit records fails.
    """


# ============================================================================
# HELPERS
# ============================================================================

def _connection_name() -> str:
    """
    Resolve the Snowflake named connection at runtime.
    """

    value = os.getenv(
        "SNOWFLAKE_CONNECTION_NAME",
        DEFAULT_CONNECTION_NAME,
    ).strip()

    if not value:

        raise ControlAuditError(
            "SNOWFLAKE_CONNECTION_NAME is empty"
        )

    return value


def _new_id() -> str:
    """
    Create a UUID value compatible with Snowflake UUID columns.
    """

    return str(
        uuid4()
    )


def _clean_error_message(
    error_message: str | None,
) -> str | None:
    """
    Keep audit errors useful without allowing extremely
    large payloads into the CONTROL tables.
    """

    if error_message is None:
        return None

    value = str(
        error_message
    ).strip()

    if not value:
        return None

    return value[:8000]


# ============================================================================
# CONTROL AUDIT CLIENT
# ============================================================================

class ControlAuditClient:
    """
    Snowflake audit client for ACT operational CONTROL tables.

    Each public write method:

        1. Opens a short-lived Snowflake connection
        2. Executes one transaction
        3. Commits on success
        4. Rolls back on failure
        5. Closes the connection
    """

    def __init__(
        self,
        connection_name: str | None = None,
    ) -> None:

        self.connection_name = (
            connection_name
            or _connection_name()
        )


    # ========================================================================
    # CONNECTION
    # ========================================================================

    def _connect(
        self,
    ) -> SnowflakeConnection:
        """
        Open Snowflake using the configured named connection.
        """

        try:

            conn = snowflake.connector.connect(
                connection_name=
                    self.connection_name,

                application=
                    "ACT_DATA_PLATFORM_CONTROL_AUDIT",
            )

            conn.autocommit(
                False
            )

            return conn

        except Exception as exc:

            logger.exception(
                (
                    "control_audit_connection_failed "
                    "connection_name=%s"
                ),
                self.connection_name,
            )

            raise ControlAuditError(
                (
                    "Unable to connect to Snowflake "
                    "for CONTROL auditing"
                )
            ) from exc


    # ========================================================================
    # TRANSACTION
    # ========================================================================

    @staticmethod
    def _execute_transaction(
        conn: SnowflakeConnection,
        sql: str,
        parameters: tuple[Any, ...],
    ) -> None:
        """
        Execute one audit DML statement transactionally.
        """

        cursor = conn.cursor()

        try:

            cursor.execute(
                sql,
                parameters,
            )

            conn.commit()

        except Exception:

            conn.rollback()

            raise

        finally:

            cursor.close()


    # ========================================================================
    # PIPELINE RUN - START
    # ========================================================================

    def start_pipeline_run(
        self,
        *,
        dag_id: str,
        dag_run_id: str,
        run_type: str = "MANUAL",
        triggered_by: str | None = None,
        studies_discovered: int | None = None,
        work_items_created: int | None = None,
    ) -> str:
        """
        Insert the RUNNING audit row for one Airflow DAG run.
        """

        audit_id = _new_id()

        sql = f"""
            INSERT INTO {PIPELINE_TABLE}
            (
                PIPELINE_AUDIT_ID,
                DAG_ID,
                DAG_RUN_ID,
                RUN_TYPE,
                TRIGGERED_BY,
                STARTED_AT,
                STATUS,
                STUDIES_DISCOVERED,
                WORK_ITEMS_CREATED,
                CREATED_AT,
                UPDATED_AT
            )
            VALUES
            (
                %s,
                %s,
                %s,
                %s,
                %s,
                CURRENT_TIMESTAMP(),
                'RUNNING',
                %s,
                %s,
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
            )
        """

        conn = self._connect()

        try:

            self._execute_transaction(
                conn,
                sql,
                (
                    audit_id,
                    dag_id,
                    dag_run_id,
                    run_type.upper(),
                    triggered_by,
                    studies_discovered,
                    work_items_created,
                ),
            )

            logger.info(
                (
                    "pipeline_audit_started "
                    "pipeline_audit_id=%s "
                    "dag_id=%s "
                    "dag_run_id=%s"
                ),
                audit_id,
                dag_id,
                dag_run_id,
            )

            return audit_id

        except Exception as exc:

            logger.exception(
                (
                    "pipeline_audit_start_failed "
                    "dag_run_id=%s"
                ),
                dag_run_id,
            )

            raise ControlAuditError(
                "Failed to start pipeline audit"
            ) from exc

        finally:

            conn.close()


    # ========================================================================
    # PIPELINE RUN - FINISH
    # ========================================================================

    def finish_pipeline_run(
        self,
        *,
        pipeline_audit_id: str,
        status: str,
        studies_discovered: int | None = None,
        work_items_created: int | None = None,
        successful_items: int | None = None,
        failed_items: int | None = None,
        error_message: str | None = None,
    ) -> None:
        """
        Complete an existing pipeline audit row.
        """

        sql = f"""
            UPDATE {PIPELINE_TABLE}
            SET
                ENDED_AT = CURRENT_TIMESTAMP(),

                STATUS = %s,

                STUDIES_DISCOVERED =
                    COALESCE(
                        %s,
                        STUDIES_DISCOVERED
                    ),

                WORK_ITEMS_CREATED =
                    COALESCE(
                        %s,
                        WORK_ITEMS_CREATED
                    ),

                SUCCESSFUL_ITEMS = %s,

                FAILED_ITEMS = %s,

                ERROR_MESSAGE = %s,

                UPDATED_AT =
                    CURRENT_TIMESTAMP()

            WHERE
                PIPELINE_AUDIT_ID = %s
        """

        conn = self._connect()

        try:

            self._execute_transaction(
                conn,
                sql,
                (
                    status.upper(),
                    studies_discovered,
                    work_items_created,
                    successful_items,
                    failed_items,
                    _clean_error_message(
                        error_message
                    ),
                    pipeline_audit_id,
                ),
            )

            logger.info(
                (
                    "pipeline_audit_finished "
                    "pipeline_audit_id=%s "
                    "status=%s"
                ),
                pipeline_audit_id,
                status.upper(),
            )

        except Exception as exc:

            logger.exception(
                (
                    "pipeline_audit_finish_failed "
                    "pipeline_audit_id=%s"
                ),
                pipeline_audit_id,
            )

            raise ControlAuditError(
                "Failed to finish pipeline audit"
            ) from exc

        finally:

            conn.close()


    # ========================================================================
    # ENTITY LOAD - START
    # ========================================================================

    def start_entity_load(
        self,
        *,
        dag_run_id: str,
        study_id: str,
        entity_name: str,
        load_type: str,
        dag_id: str | None = None,
        task_id: str | None = None,
        map_index: int | None = None,
        attempt_number: int | None = None,
        source_watermark_from: Any | None = None,
    ) -> str:
        """
        Insert the RUNNING row for one study/entity load attempt.
        """

        audit_id = _new_id()

        sql = f"""
            INSERT INTO {ENTITY_TABLE}
            (
                ENTITY_LOAD_AUDIT_ID,
                DAG_ID,
                DAG_RUN_ID,
                TASK_ID,
                MAP_INDEX,
                ATTEMPT_NUMBER,
                STUDY_ID,
                ENTITY_NAME,
                LOAD_TYPE,
                STARTED_AT,
                STATUS,
                SOURCE_WATERMARK_FROM,
                CREATED_AT,
                UPDATED_AT
            )
            VALUES
            (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                CURRENT_TIMESTAMP(),
                'RUNNING',
                %s,
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
            )
        """

        conn = self._connect()

        try:

            self._execute_transaction(
                conn,
                sql,
                (
                    audit_id,
                    dag_id,
                    dag_run_id,
                    task_id,
                    map_index,
                    attempt_number,
                    study_id.upper(),
                    entity_name.lower(),
                    load_type.upper(),
                    source_watermark_from,
                ),
            )

            logger.info(
                (
                    "entity_audit_started "
                    "entity_load_audit_id=%s "
                    "study_id=%s "
                    "entity=%s "
                    "load_type=%s"
                ),
                audit_id,
                study_id.upper(),
                entity_name.lower(),
                load_type.upper(),
            )

            return audit_id

        except Exception as exc:

            logger.exception(
                (
                    "entity_audit_start_failed "
                    "study_id=%s "
                    "entity=%s"
                ),
                study_id,
                entity_name,
            )

            raise ControlAuditError(
                "Failed to start entity-load audit"
            ) from exc

        finally:

            conn.close()


    # ========================================================================
    # ENTITY LOAD - FINISH
    # ========================================================================

    def finish_entity_load(
        self,
        *,
        entity_load_audit_id: str,
        status: str,
        source_row_count: int | None = None,
        storage_row_count: int | None = None,
        snowflake_row_count: int | None = None,
        source_watermark_to: Any | None = None,
        storage_uri: str | None = None,
        file_checksum: str | None = None,
        error_message: str | None = None,
    ) -> None:
        """
        Complete one study/entity load audit.

        STORAGE_ROW_COUNT represents the number of normalized
        rows successfully written to the configured storage
        backend.

        STORAGE_URI may contain, for example:

            file:///...
            @ACT_DB.RAW.ACT_RAW_STAGE/...
            s3://...
            abfss://...

        The CONTROL layer therefore remains independent from
        the physical storage technology.
        """

        sql = f"""
            UPDATE {ENTITY_TABLE}
            SET
                ENDED_AT =
                    CURRENT_TIMESTAMP(),

                STATUS = %s,

                SOURCE_ROW_COUNT = %s,

                STORAGE_ROW_COUNT = %s,

                SNOWFLAKE_ROW_COUNT = %s,

                SOURCE_WATERMARK_TO = %s,

                STORAGE_URI = %s,

                FILE_CHECKSUM = %s,

                ERROR_MESSAGE = %s,

                UPDATED_AT =
                    CURRENT_TIMESTAMP()

            WHERE
                ENTITY_LOAD_AUDIT_ID = %s
        """

        conn = self._connect()

        try:

            self._execute_transaction(
                conn,
                sql,
                (
                    status.upper(),
                    source_row_count,
                    storage_row_count,
                    snowflake_row_count,
                    source_watermark_to,
                    storage_uri,
                    file_checksum,
                    _clean_error_message(
                        error_message
                    ),
                    entity_load_audit_id,
                ),
            )

            logger.info(
                (
                    "entity_audit_finished "
                    "entity_load_audit_id=%s "
                    "status=%s"
                ),
                entity_load_audit_id,
                status.upper(),
            )

        except Exception as exc:

            logger.exception(
                (
                    "entity_audit_finish_failed "
                    "entity_load_audit_id=%s"
                ),
                entity_load_audit_id,
            )

            raise ControlAuditError(
                "Failed to finish entity-load audit"
            ) from exc

        finally:

            conn.close()


    # ========================================================================
    # REPROCESS - START
    # ========================================================================

    def start_reprocess(
        self,
        *,
        study_id: str,
        entity_name: str,
        reprocess_from: Any,
        reprocess_to: Any | None = None,
        dag_run_id: str | None = None,
        requested_by: str | None = None,
        normal_watermark_before: Any | None = None,
    ) -> str:
        """
        Insert the RUNNING row for one manual historical
        reprocess request.
        """

        audit_id = _new_id()

        sql = f"""
            INSERT INTO {REPROCESS_TABLE}
            (
                REPROCESS_AUDIT_ID,
                DAG_RUN_ID,
                STUDY_ID,
                ENTITY_NAME,
                REPROCESS_FROM,
                REPROCESS_TO,
                REQUESTED_AT,
                REQUESTED_BY,
                STARTED_AT,
                STATUS,
                NORMAL_WATERMARK_BEFORE,
                CREATED_AT,
                UPDATED_AT
            )
            VALUES
            (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                CURRENT_TIMESTAMP(),
                %s,
                CURRENT_TIMESTAMP(),
                'RUNNING',
                %s,
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
            )
        """

        conn = self._connect()

        try:

            self._execute_transaction(
                conn,
                sql,
                (
                    audit_id,
                    dag_run_id,
                    study_id.upper(),
                    entity_name.lower(),
                    reprocess_from,
                    reprocess_to,
                    requested_by,
                    normal_watermark_before,
                ),
            )

            logger.info(
                (
                    "reprocess_audit_started "
                    "reprocess_audit_id=%s "
                    "study_id=%s "
                    "entity=%s"
                ),
                audit_id,
                study_id.upper(),
                entity_name.lower(),
            )

            return audit_id

        except Exception as exc:

            logger.exception(
                (
                    "reprocess_audit_start_failed "
                    "study_id=%s "
                    "entity=%s"
                ),
                study_id,
                entity_name,
            )

            raise ControlAuditError(
                "Failed to start reprocess audit"
            ) from exc

        finally:

            conn.close()


    # ========================================================================
    # REPROCESS - FINISH
    # ========================================================================

    def finish_reprocess(
        self,
        *,
        reprocess_audit_id: str,
        status: str,
        source_row_count: int | None = None,
        storage_uri: str | None = None,
        file_checksum: str | None = None,
        normal_watermark_after: Any | None = None,
        error_message: str | None = None,
    ) -> None:
        """
        Complete an existing manual reprocess audit row.
        """

        sql = f"""
            UPDATE {REPROCESS_TABLE}
            SET
                ENDED_AT =
                    CURRENT_TIMESTAMP(),

                STATUS = %s,

                SOURCE_ROW_COUNT = %s,

                STORAGE_URI = %s,

                FILE_CHECKSUM = %s,

                NORMAL_WATERMARK_AFTER = %s,

                ERROR_MESSAGE = %s,

                UPDATED_AT =
                    CURRENT_TIMESTAMP()

            WHERE
                REPROCESS_AUDIT_ID = %s
        """

        conn = self._connect()

        try:

            self._execute_transaction(
                conn,
                sql,
                (
                    status.upper(),
                    source_row_count,
                    storage_uri,
                    file_checksum,
                    normal_watermark_after,
                    _clean_error_message(
                        error_message
                    ),
                    reprocess_audit_id,
                ),
            )

            logger.info(
                (
                    "reprocess_audit_finished "
                    "reprocess_audit_id=%s "
                    "status=%s"
                ),
                reprocess_audit_id,
                status.upper(),
            )

        except Exception as exc:

            logger.exception(
                (
                    "reprocess_audit_finish_failed "
                    "reprocess_audit_id=%s"
                ),
                reprocess_audit_id,
            )

            raise ControlAuditError(
                "Failed to finish reprocess audit"
            ) from exc

        finally:

            conn.close()