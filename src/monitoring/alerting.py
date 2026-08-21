from __future__ import annotations

import logging
import os
import uuid

from datetime import datetime
from typing import Any

import snowflake.connector

from src.monitoring.slack_notifier import (
    send_failure_notification,
    send_recovery_notification,
)


logger = logging.getLogger(__name__)


# ============================================================
# CONFIGURATION
# ============================================================

DEFAULT_CONNECTION_NAME = "SNOWFLAKE_ACT_DEV"

ALERT_TABLE = "ACT_DB.CONTROL.PIPELINE_ALERT"

MAX_ERROR_MESSAGE_LENGTH = 16000


# ============================================================
# CONTEXT HELPERS
# ============================================================

def _context_values(
    context: dict[str, Any],
) -> dict[str, Any]:
    """
    Extract common Airflow callback values from a context.

    Works for normal and dynamically mapped tasks.
    """

    task_instance = (
        context.get("task_instance")
        or context.get("ti")
    )

    task = context.get("task")

    dag_run = context.get("dag_run")

    exception = context.get("exception")

    # ========================================================
    # DAG ID
    # ========================================================

    dag_id = None

    if task_instance is not None:
        dag_id = getattr(
            task_instance,
            "dag_id",
            None,
        )

    if dag_id is None and task is not None:
        dag_id = getattr(
            task,
            "dag_id",
            None,
        )

    # ========================================================
    # TASK ID
    # ========================================================

    task_id = "unknown_task"

    if task_instance is not None:
        task_id = (
            getattr(
                task_instance,
                "task_id",
                None,
            )
            or task_id
        )

    elif task is not None:
        task_id = (
            getattr(
                task,
                "task_id",
                None,
            )
            or task_id
        )

    # ========================================================
    # DAG RUN ID
    # ========================================================

    dag_run_id = None

    if dag_run is not None:
        dag_run_id = getattr(
            dag_run,
            "run_id",
            None,
        )

    if dag_run_id is None and task_instance is not None:
        dag_run_id = getattr(
            task_instance,
            "run_id",
            None,
        )

    # ========================================================
    # MAP INDEX
    # ========================================================

    map_index = None

    if task_instance is not None:
        map_index_value = getattr(
            task_instance,
            "map_index",
            None,
        )

        if (
            map_index_value is not None
            and int(map_index_value) >= 0
        ):
            map_index = int(
                map_index_value
            )

    # ========================================================
    # LOGICAL DATE
    # ========================================================

    logical_date = context.get(
        "logical_date"
    )

    if logical_date is None and dag_run is not None:
        logical_date = getattr(
            dag_run,
            "logical_date",
            None,
        )

    return {
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "task_id": task_id,
        "map_index": map_index,
        "logical_date": logical_date,
        "exception": exception,
    }


# ============================================================
# ALERT CLASSIFICATION
# ============================================================

def classify_alert(
    task_id: str,
) -> tuple[str, str]:
    """
    Determine alert type and severity from the failed task.

    Returns:
        (alert_type, severity)
    """

    normalized_task_id = (
        task_id
        .strip()
        .lower()
    )

    if normalized_task_id == "run_dbt_build":
        return (
            "DBT_FAILURE",
            "CRITICAL",
        )

    if normalized_task_id == "reconcile_pipeline":
        return (
            "RECONCILIATION_FAILURE",
            "CRITICAL",
        )

    if normalized_task_id == "process_snowflake_raw_entity":
        return (
            "RAW_PROCESSING_FAILURE",
            "ERROR",
        )

    if normalized_task_id == "ingest_study_entity":
        return (
            "INGESTION_FAILURE",
            "ERROR",
        )

    if normalized_task_id == "discover_studies":
        return (
            "SOURCE_DISCOVERY_FAILURE",
            "ERROR",
        )

    if normalized_task_id == "finish_pipeline_audit":
        return (
            "AUDIT_FAILURE",
            "ERROR",
        )

    return (
        "TASK_FAILURE",
        "ERROR",
    )


# ============================================================
# ERROR MESSAGE
# ============================================================

def build_error_message(
    exception: BaseException | None,
) -> str | None:
    """
    Convert the Airflow exception into a bounded message
    suitable for Snowflake storage.
    """

    if exception is None:
        return None

    message = (
        f"{type(exception).__name__}: "
        f"{exception}"
    )

    return message[
        :MAX_ERROR_MESSAGE_LENGTH
    ]


# ============================================================
# CONNECTION
# ============================================================

def _connection_name(
    connection_name: str | None = None,
) -> str:
    return (
        connection_name
        or os.getenv(
            "SNOWFLAKE_CONNECTION_NAME"
        )
        or DEFAULT_CONNECTION_NAME
    )


# ============================================================
# CREATE ALERT
# ============================================================

def write_pipeline_alert(
    *,
    dag_id: str | None,
    dag_run_id: str | None,
    task_id: str,
    map_index: int | None,
    alert_type: str,
    severity: str,
    error_message: str | None,
    logical_date: datetime | None,
    connection_name: str | None = None,
) -> str:
    """
    Insert one OPEN operational alert.

    Returns:
        Generated ALERT_ID.
    """

    alert_id = str(
        uuid.uuid4()
    )

    resolved_connection_name = (
        _connection_name(
            connection_name
        )
    )

    logger.info(
        (
            "pipeline_alert_write_started "
            "alert_id=%s "
            "dag_id=%s "
            "dag_run_id=%s "
            "task_id=%s "
            "map_index=%s "
            "alert_type=%s "
            "severity=%s"
        ),
        alert_id,
        dag_id,
        dag_run_id,
        task_id,
        map_index,
        alert_type,
        severity,
    )

    connection = (
        snowflake.connector.connect(
            connection_name=
                resolved_connection_name,
            application=
                "ACT_DATA_PLATFORM_ALERTING",
        )
    )

    try:
        cursor = connection.cursor()

        try:
            sql = f"""
                INSERT INTO {ALERT_TABLE}
                (
                    ALERT_ID,
                    DAG_ID,
                    DAG_RUN_ID,
                    TASK_ID,
                    MAP_INDEX,
                    ALERT_TYPE,
                    SEVERITY,
                    STATUS,
                    ERROR_MESSAGE,
                    LOGICAL_DATE,
                    CREATED_AT,
                    RESOLVED_AT,
                    RESOLVED_BY_DAG_RUN_ID
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
                    'OPEN',
                    %s,
                    %s,
                    CURRENT_TIMESTAMP(),
                    NULL,
                    NULL
                )
            """

            cursor.execute(
                sql,
                (
                    alert_id,
                    dag_id,
                    dag_run_id,
                    task_id,
                    map_index,
                    alert_type,
                    severity,
                    error_message,
                    logical_date,
                ),
            )

            connection.commit()

        finally:
            cursor.close()

    finally:
        connection.close()

    logger.error(
        (
            "pipeline_alert_created "
            "alert_id=%s "
            "dag_id=%s "
            "dag_run_id=%s "
            "task_id=%s "
            "alert_type=%s "
            "severity=%s"
        ),
        alert_id,
        dag_id,
        dag_run_id,
        task_id,
        alert_type,
        severity,
    )

    return alert_id


# ============================================================
# RESOLVE ALERTS
# ============================================================

def resolve_pipeline_alerts(
    *,
    dag_id: str | None,
    dag_run_id: str | None,
    task_id: str,
    map_index: int | None,
    connection_name: str | None = None,
) -> int:
    """
    Resolve previous OPEN alerts for the same Airflow task.

    Matching rules:

      normal task:
          DAG_ID + TASK_ID + MAP_INDEX IS NULL

      mapped task:
          DAG_ID + TASK_ID + MAP_INDEX

    This allows a later successful retry/rerun to close the
    operational alert created by an earlier failed execution.

    Returns:
        Number of alerts resolved.
    """

    if not dag_id:
        logger.warning(
            (
                "pipeline_alert_resolution_skipped "
                "reason=missing_dag_id "
                "task_id=%s"
            ),
            task_id,
        )
        return 0

    resolved_connection_name = (
        _connection_name(
            connection_name
        )
    )

    connection = (
        snowflake.connector.connect(
            connection_name=
                resolved_connection_name,
            application=
                "ACT_DATA_PLATFORM_ALERTING",
        )
    )

    try:
        cursor = connection.cursor()

        try:
            if map_index is None:
                sql = f"""
                    UPDATE {ALERT_TABLE}
                    SET
                        STATUS = 'RESOLVED',
                        RESOLVED_AT = CURRENT_TIMESTAMP(),
                        RESOLVED_BY_DAG_RUN_ID = %s
                    WHERE STATUS = 'OPEN'
                      AND DAG_ID = %s
                      AND TASK_ID = %s
                      AND MAP_INDEX IS NULL
                """

                parameters = (
                    dag_run_id,
                    dag_id,
                    task_id,
                )

            else:
                sql = f"""
                    UPDATE {ALERT_TABLE}
                    SET
                        STATUS = 'RESOLVED',
                        RESOLVED_AT = CURRENT_TIMESTAMP(),
                        RESOLVED_BY_DAG_RUN_ID = %s
                    WHERE STATUS = 'OPEN'
                      AND DAG_ID = %s
                      AND TASK_ID = %s
                      AND MAP_INDEX = %s
                """

                parameters = (
                    dag_run_id,
                    dag_id,
                    task_id,
                    map_index,
                )

            cursor.execute(
                sql,
                parameters,
            )

            resolved_count = int(
                cursor.rowcount
                if cursor.rowcount is not None
                and cursor.rowcount >= 0
                else 0
            )

            connection.commit()

        finally:
            cursor.close()

    finally:
        connection.close()

    if resolved_count > 0:
        logger.info(
            (
                "pipeline_alerts_resolved "
                "dag_id=%s "
                "dag_run_id=%s "
                "task_id=%s "
                "map_index=%s "
                "resolved_count=%s"
            ),
            dag_id,
            dag_run_id,
            task_id,
            map_index,
            resolved_count,
        )

    return resolved_count


# ============================================================
# AIRFLOW FAILURE CALLBACK
# ============================================================

def on_task_failure(
    context: dict[str, Any],
) -> None:
    """
    Persist an OPEN alert when an Airflow task fails.

    Alerting failure must never hide the original task failure.
    """

    try:
        values = _context_values(
            context
        )

        alert_type, severity = (
            classify_alert(
                values["task_id"]
            )
        )

        error_message = (
            build_error_message(
                values["exception"]
            )
        )

        alert_id = (
            write_pipeline_alert(
                dag_id=
                    values["dag_id"],
                dag_run_id=
                    values["dag_run_id"],
                task_id=
                    values["task_id"],
                map_index=
                    values["map_index"],
                alert_type=
                    alert_type,
                severity=
                    severity,
                error_message=
                    error_message,
                logical_date=
                    values["logical_date"],
            )
        )

        logger.error(
            (
                "airflow_task_failure_alert_recorded "
                "alert_id=%s "
                "task_id=%s"
            ),
            alert_id,
            values["task_id"],
        )

        send_failure_notification(
            alert_id=alert_id,
            dag_id=values["dag_id"],
            dag_run_id=values["dag_run_id"],
            task_id=values["task_id"],
            map_index=values["map_index"],
            alert_type=alert_type,
            severity=severity,
            error_message=error_message,
        )

    except Exception:
        logger.exception(
            (
                "Unable to persist Airflow failure alert. "
                "Original task failure remains unchanged."
            )
        )


# ============================================================
# AIRFLOW SUCCESS CALLBACK
# ============================================================

def on_task_success(
    context: dict[str, Any],
) -> None:
    """
    Resolve matching OPEN alerts when an Airflow task succeeds.

    Resolution failure is logged but does not turn a successful
    pipeline task into a failure.
    """

    try:
        values = _context_values(
            context
        )

        resolved_count = (
            resolve_pipeline_alerts(
                dag_id=
                    values["dag_id"],
                dag_run_id=
                    values["dag_run_id"],
                task_id=
                    values["task_id"],
                map_index=
                    values["map_index"],
            )
        )

        logger.info(
            (
                "airflow_task_success_alert_resolution "
                "dag_id=%s "
                "dag_run_id=%s "
                "task_id=%s "
                "map_index=%s "
                "resolved_count=%s"
            ),
            values["dag_id"],
            values["dag_run_id"],
            values["task_id"],
            values["map_index"],
            resolved_count,
        )

        if resolved_count > 0:
            send_recovery_notification(
                dag_id=values["dag_id"],
                dag_run_id=values["dag_run_id"],
                task_id=values["task_id"],
                map_index=values["map_index"],
                resolved_count=resolved_count,
            )

    except Exception:
        logger.exception(
            (
                "Unable to resolve prior Airflow alerts. "
                "Successful task state remains unchanged."
            )
        )
