from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import snowflake.connector
import streamlit as st


# ============================================================
# CONFIGURATION
# ============================================================

DEFAULT_CONNECTION_NAME = "SNOWFLAKE_ACT_DEV"

DATABASE = os.getenv(
    "ACT_SNOWFLAKE_DATABASE",
    "ACT_DB",
)

CONTROL_SCHEMA = os.getenv(
    "ACT_CONTROL_SCHEMA",
    "CONTROL",
)

PIPELINE_AUDIT_TABLE = (
    f"{DATABASE}.{CONTROL_SCHEMA}.PIPELINE_RUN_AUDIT"
)

ENTITY_AUDIT_TABLE = (
    f"{DATABASE}.{CONTROL_SCHEMA}.ENTITY_LOAD_AUDIT"
)

PIPELINE_ALERT_TABLE = (
    f"{DATABASE}.{CONTROL_SCHEMA}.PIPELINE_ALERT"
)


# ============================================================
# STREAMLIT PAGE
# ============================================================

st.set_page_config(
    page_title="ACT Operations Dashboard",
    page_icon="📊",
    layout="wide",
)

st.title(
    "ACT Data Platform — Operations Dashboard"
)

st.caption(
    "Airflow pipeline health, Snowflake load audit, "
    "reconciliation counts and operational alerts."
)


# ============================================================
# HELPERS
# ============================================================

def get_connection_name() -> str:
    """
    Resolve the Snowflake named connection.

    Credentials remain outside the dashboard source code.
    """

    return (
        os.getenv("SNOWFLAKE_CONNECTION_NAME")
        or DEFAULT_CONNECTION_NAME
    )


def cursor_to_dataframe(
    cursor: Any,
) -> pd.DataFrame:
    """
    Convert the result from the current cursor execution
    into a pandas DataFrame.
    """

    rows = cursor.fetchall()

    columns = [
        column[0]
        for column in cursor.description
    ]

    return pd.DataFrame(
        rows,
        columns=columns,
    )


def execute_dataframe(
    cursor: Any,
    sql: str,
) -> pd.DataFrame:
    """
    Execute one query using an already-open cursor.

    Reusing one Snowflake connection for all dashboard queries
    avoids repeatedly reconnecting to Snowflake.
    """

    cursor.execute(
        sql
    )

    return cursor_to_dataframe(
        cursor
    )


def safe_int(
    value: Any,
) -> int:
    """
    Convert Snowflake/pandas numeric values safely to int.
    """

    if value is None:
        return 0

    try:
        if pd.isna(value):
            return 0
    except TypeError:
        pass

    return int(
        value
    )


def display_value(
    value: Any,
    fallback: str = "—",
) -> str:
    """
    Convert nullable values to display-safe strings.
    """

    if value is None:
        return fallback

    try:
        if pd.isna(value):
            return fallback
    except TypeError:
        pass

    return str(
        value
    )


def duration_text(
    seconds: Any,
) -> str:
    """
    Convert seconds into a compact duration.
    """

    if seconds is None:
        return "—"

    try:
        if pd.isna(seconds):
            return "—"
    except TypeError:
        pass

    total_seconds = max(
        int(seconds),
        0,
    )

    hours, remainder = divmod(
        total_seconds,
        3600,
    )

    minutes, seconds = divmod(
        remainder,
        60,
    )

    if hours:
        return (
            f"{hours}h {minutes}m {seconds}s"
        )

    if minutes:
        return (
            f"{minutes}m {seconds}s"
        )

    return (
        f"{seconds}s"
    )


def format_timestamp(
    value: Any,
) -> str:
    """
    Format a Snowflake timestamp for display.
    """

    if value is None:
        return "—"

    try:
        if pd.isna(value):
            return "—"
    except TypeError:
        pass

    timestamp = pd.Timestamp(
        value
    )

    return timestamp.strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def status_label(
    status: Any,
) -> str:
    """
    Add a simple visual marker to a pipeline status.
    """

    normalized = (
        display_value(
            status,
            fallback="UNKNOWN",
        )
        .strip()
        .upper()
    )

    if normalized == "SUCCESS":
        return "✅ SUCCESS"

    if normalized == "FAILED":
        return "❌ FAILED"

    if normalized in {
        "RUNNING",
        "STARTED",
    }:
        return f"🔄 {normalized}"

    return f"ℹ️ {normalized}"


# ============================================================
# DATA LOAD
# ============================================================

@st.cache_data(
    ttl=30,
    show_spinner=False,
)
def load_dashboard_data() -> dict[str, pd.DataFrame]:
    """
    Load all dashboard datasets using ONE Snowflake connection.

    This is intentionally different from opening one connection
    for every chart/query. A single connection makes local
    Streamlit refreshes much faster.
    """

    connection = snowflake.connector.connect(
        connection_name=get_connection_name(),
        application="ACT_STREAMLIT_OPERATIONS_DASHBOARD",
    )

    try:
        cursor = connection.cursor()

        try:
            latest_pipeline = execute_dataframe(
                cursor,
                f"""
                SELECT
                    PIPELINE_AUDIT_ID,
                    DAG_ID,
                    DAG_RUN_ID,
                    RUN_TYPE,
                    TRIGGERED_BY,
                    STARTED_AT,
                    ENDED_AT,
                    STATUS,
                    STUDIES_DISCOVERED,
                    WORK_ITEMS_CREATED,
                    SUCCESSFUL_ITEMS,
                    FAILED_ITEMS,
                    ERROR_MESSAGE,
                    DATEDIFF(
                        'second',
                        STARTED_AT,
                        COALESCE(
                            ENDED_AT,
                            CURRENT_TIMESTAMP()
                        )
                    ) AS DURATION_SECONDS
                FROM {PIPELINE_AUDIT_TABLE}
                ORDER BY STARTED_AT DESC
                LIMIT 1
                """
            )

            pipeline_history = execute_dataframe(
                cursor,
                f"""
                SELECT
                    DAG_RUN_ID,
                    RUN_TYPE,
                    STARTED_AT,
                    ENDED_AT,
                    STATUS,
                    STUDIES_DISCOVERED,
                    WORK_ITEMS_CREATED,
                    SUCCESSFUL_ITEMS,
                    FAILED_ITEMS,
                    DATEDIFF(
                        'second',
                        STARTED_AT,
                        COALESCE(
                            ENDED_AT,
                            CURRENT_TIMESTAMP()
                        )
                    ) AS DURATION_SECONDS,
                    ERROR_MESSAGE
                FROM {PIPELINE_AUDIT_TABLE}
                ORDER BY STARTED_AT DESC
                LIMIT 50
                """
            )

            alert_summary = execute_dataframe(
                cursor,
                f"""
                SELECT
                    COUNT_IF(
                        STATUS = 'OPEN'
                    ) AS OPEN_ALERTS,

                    COUNT_IF(
                        STATUS = 'OPEN'
                        AND SEVERITY = 'CRITICAL'
                    ) AS OPEN_CRITICAL_ALERTS,

                    COUNT_IF(
                        STATUS = 'RESOLVED'
                    ) AS RESOLVED_ALERTS

                FROM {PIPELINE_ALERT_TABLE}
                """
            )

            alerts = execute_dataframe(
                cursor,
                f"""
                SELECT
                    ALERT_ID,
                    DAG_RUN_ID,
                    TASK_ID,
                    MAP_INDEX,
                    ALERT_TYPE,
                    SEVERITY,
                    STATUS,
                    ERROR_MESSAGE,
                    CREATED_AT,
                    RESOLVED_AT,
                    RESOLVED_BY_DAG_RUN_ID
                FROM {PIPELINE_ALERT_TABLE}
                ORDER BY CREATED_AT DESC
                LIMIT 100
                """
            )

            latest_entity_summary = execute_dataframe(
                cursor,
                f"""
                WITH latest_run AS
                (
                    SELECT DAG_RUN_ID
                    FROM {PIPELINE_AUDIT_TABLE}
                    ORDER BY STARTED_AT DESC
                    LIMIT 1
                )

                SELECT
                    e.ENTITY_NAME,

                    COUNT(*) AS LOAD_ATTEMPTS,

                    COUNT_IF(
                        e.STATUS = 'SUCCESS'
                    ) AS SUCCESSFUL_LOADS,

                    COUNT_IF(
                        e.STATUS <> 'SUCCESS'
                        OR e.STATUS IS NULL
                    ) AS FAILED_LOADS,

                    COALESCE(
                        SUM(e.SOURCE_ROW_COUNT),
                        0
                    ) AS SOURCE_ROWS,

                    COALESCE(
                        SUM(e.STORAGE_ROW_COUNT),
                        0
                    ) AS STORAGE_ROWS,

                    COALESCE(
                        SUM(e.SNOWFLAKE_ROW_COUNT),
                        0
                    ) AS SNOWFLAKE_ROWS,

                    MAX(
                        e.ENDED_AT
                    ) AS LAST_COMPLETED_AT

                FROM {ENTITY_AUDIT_TABLE} e

                INNER JOIN latest_run r
                    ON e.DAG_RUN_ID = r.DAG_RUN_ID

                GROUP BY
                    e.ENTITY_NAME

                ORDER BY
                    e.ENTITY_NAME
                """
            )

            recent_entity_failures = execute_dataframe(
                cursor,
                f"""
                SELECT
                    DAG_RUN_ID,
                    STUDY_ID,
                    ENTITY_NAME,
                    TASK_ID,
                    MAP_INDEX,
                    ATTEMPT_NUMBER,
                    STATUS,
                    ERROR_MESSAGE,
                    STARTED_AT,
                    ENDED_AT
                FROM {ENTITY_AUDIT_TABLE}
                WHERE STATUS <> 'SUCCESS'
                   OR STATUS IS NULL
                ORDER BY STARTED_AT DESC
                LIMIT 50
                """
            )

            entity_history = execute_dataframe(
                cursor,
                f"""
                SELECT
                    ENTITY_NAME,

                    COUNT(*) AS TOTAL_LOAD_ATTEMPTS,

                    COUNT_IF(
                        STATUS = 'SUCCESS'
                    ) AS SUCCESSFUL_LOADS,

                    COUNT_IF(
                        STATUS <> 'SUCCESS'
                        OR STATUS IS NULL
                    ) AS FAILED_LOADS,

                    COALESCE(
                        SUM(SOURCE_ROW_COUNT),
                        0
                    ) AS SOURCE_ROWS,

                    COALESCE(
                        SUM(STORAGE_ROW_COUNT),
                        0
                    ) AS STORAGE_ROWS,

                    COALESCE(
                        SUM(SNOWFLAKE_ROW_COUNT),
                        0
                    ) AS SNOWFLAKE_ROWS

                FROM {ENTITY_AUDIT_TABLE}

                GROUP BY
                    ENTITY_NAME

                ORDER BY
                    ENTITY_NAME
                """
            )

        finally:
            cursor.close()

    finally:
        connection.close()

    return {
        "latest_pipeline": latest_pipeline,
        "pipeline_history": pipeline_history,
        "alert_summary": alert_summary,
        "alerts": alerts,
        "latest_entity_summary": latest_entity_summary,
        "recent_entity_failures": recent_entity_failures,
        "entity_history": entity_history,
    }


# ============================================================
# SIDEBAR
# ============================================================

with st.sidebar:
    st.header(
        "Dashboard Controls"
    )

    st.write(
        "Snowflake connection:"
    )

    st.code(
        get_connection_name(),
        language=None,
    )

    if st.button(
        "Refresh now",
        width="stretch",
    ):
        st.cache_data.clear()
        st.rerun()

    st.caption(
        "Dashboard queries are cached for 30 seconds."
    )


# ============================================================
# LOAD DASHBOARD
# ============================================================

with st.spinner(
    "Loading operational data from Snowflake..."
):
    try:
        data = load_dashboard_data()

    except Exception as exc:
        st.error(
            "Unable to load ACT operational dashboard data."
        )

        st.exception(
            exc
        )

        st.stop()


latest_pipeline = data[
    "latest_pipeline"
]

pipeline_history = data[
    "pipeline_history"
]

alert_summary = data[
    "alert_summary"
]

alerts = data[
    "alerts"
]

latest_entity_summary = data[
    "latest_entity_summary"
]

recent_entity_failures = data[
    "recent_entity_failures"
]

entity_history = data[
    "entity_history"
]


# ============================================================
# CURRENT PIPELINE HEALTH
# ============================================================

st.subheader(
    "Current Pipeline Health"
)

if latest_pipeline.empty:
    st.warning(
        "No rows found in PIPELINE_RUN_AUDIT."
    )

else:
    latest = latest_pipeline.iloc[0]

    open_alerts = 0
    critical_alerts = 0

    if not alert_summary.empty:
        alert_row = alert_summary.iloc[0]

        open_alerts = safe_int(
            alert_row.get(
                "OPEN_ALERTS"
            )
        )

        critical_alerts = safe_int(
            alert_row.get(
                "OPEN_CRITICAL_ALERTS"
            )
        )

    metric1, metric2, metric3, metric4, metric5 = (
        st.columns(
            5
        )
    )

    metric1.metric(
        "Latest Pipeline",
        status_label(
            latest.get(
                "STATUS"
            )
        ),
    )

    metric2.metric(
        "Run Duration",
        duration_text(
            latest.get(
                "DURATION_SECONDS"
            )
        ),
    )

    metric3.metric(
        "Successful Items",
        safe_int(
            latest.get(
                "SUCCESSFUL_ITEMS"
            )
        ),
    )

    metric4.metric(
        "Failed Items",
        safe_int(
            latest.get(
                "FAILED_ITEMS"
            )
        ),
    )

    metric5.metric(
        "Open Alerts",
        open_alerts,
        delta=(
            f"{critical_alerts} critical"
            if critical_alerts > 0
            else None
        ),
        delta_color="inverse",
    )

    st.write(
        "**Latest DAG Run:**",
        display_value(
            latest.get(
                "DAG_RUN_ID"
            )
        ),
    )

    st.caption(
        "Started: "
        f"{format_timestamp(latest.get('STARTED_AT'))}"
        "  •  "
        "Ended: "
        f"{format_timestamp(latest.get('ENDED_AT'))}"
    )

    if (
        display_value(
            latest.get(
                "STATUS"
            )
        ).upper()
        == "FAILED"
    ):
        error_message = latest.get(
            "ERROR_MESSAGE"
        )

        if (
            error_message is not None
            and not pd.isna(
                error_message
            )
        ):
            st.error(
                display_value(
                    error_message
                )
            )


# ============================================================
# MAIN DASHBOARD TABS
# ============================================================

tab_overview, tab_entities, tab_alerts, tab_history = st.tabs(
    [
        "Overview",
        "Entity Processing",
        "Alerts",
        "Run History",
    ]
)


# ============================================================
# OVERVIEW
# ============================================================

with tab_overview:
    st.subheader(
        "Latest Run — Entity Reconciliation"
    )

    if latest_entity_summary.empty:
        st.info(
            "The latest pipeline run has no ENTITY_LOAD_AUDIT "
            "rows. This is expected when a run fails before "
            "entity ingestion begins."
        )

    else:
        summary_display = (
            latest_entity_summary.copy()
        )

        st.dataframe(
            summary_display,
            width="stretch",
            hide_index=True,
        )

        count_chart = (
            summary_display[
                [
                    "ENTITY_NAME",
                    "SOURCE_ROWS",
                    "STORAGE_ROWS",
                    "SNOWFLAKE_ROWS",
                ]
            ]
            .set_index(
                "ENTITY_NAME"
            )
        )

        st.caption(
            "Source → Storage → Snowflake row-count comparison"
        )

        st.bar_chart(
            count_chart,
        )


# ============================================================
# ENTITY PROCESSING
# ============================================================

with tab_entities:
    st.subheader(
        "Entity Processing History"
    )

    if entity_history.empty:
        st.info(
            "No ENTITY_LOAD_AUDIT history is available."
        )

    else:
        st.dataframe(
            entity_history,
            width="stretch",
            hide_index=True,
        )

    st.subheader(
        "Recent Entity Load Failures"
    )

    if recent_entity_failures.empty:
        st.success(
            "No failed entity-load audit rows found."
        )

    else:
        st.dataframe(
            recent_entity_failures,
            width="stretch",
            hide_index=True,
        )


# ============================================================
# ALERTS
# ============================================================

with tab_alerts:
    st.subheader(
        "Operational Alerts"
    )

    filter_column, summary_column = st.columns(
        [
            2,
            3,
        ]
    )

    with filter_column:
        alert_filter = st.radio(
            "Alert status",
            options=[
                "OPEN",
                "RESOLVED",
                "ALL",
            ],
            horizontal=True,
        )

    with summary_column:
        if not alert_summary.empty:
            alert_row = alert_summary.iloc[0]

            st.write(
                "**Open:**",
                safe_int(
                    alert_row.get(
                        "OPEN_ALERTS"
                    )
                ),
                "  |  "
                "**Critical Open:**",
                safe_int(
                    alert_row.get(
                        "OPEN_CRITICAL_ALERTS"
                    )
                ),
                "  |  "
                "**Resolved:**",
                safe_int(
                    alert_row.get(
                        "RESOLVED_ALERTS"
                    )
                ),
            )

    filtered_alerts = (
        alerts.copy()
    )

    if alert_filter != "ALL":
        filtered_alerts = filtered_alerts[
            filtered_alerts[
                "STATUS"
            ] == alert_filter
        ]

    if filtered_alerts.empty:
        st.success(
            (
                f"No {alert_filter.lower()} operational alerts."
                if alert_filter != "ALL"
                else "No operational alerts."
            )
        )

    else:
        st.dataframe(
            filtered_alerts,
            width="stretch",
            hide_index=True,
        )


# ============================================================
# RUN HISTORY
# ============================================================

with tab_history:
    st.subheader(
        "Pipeline Run History"
    )

    if pipeline_history.empty:
        st.info(
            "No pipeline history is available."
        )

    else:
        history_display = (
            pipeline_history.copy()
        )

        history_display[
            "DURATION"
        ] = history_display[
            "DURATION_SECONDS"
        ].apply(
            duration_text
        )

        st.dataframe(
            history_display.drop(
                columns=[
                    "DURATION_SECONDS",
                ]
            ),
            width="stretch",
            hide_index=True,
        )

        duration_chart = (
            pipeline_history[
                [
                    "STARTED_AT",
                    "DURATION_SECONDS",
                ]
            ]
            .dropna()
            .sort_values(
                "STARTED_AT"
            )
            .set_index(
                "STARTED_AT"
            )
        )

        if not duration_chart.empty:
            st.caption(
                "Pipeline duration trend in seconds"
            )

            st.line_chart(
                duration_chart,
                y="DURATION_SECONDS",
            )


# ============================================================
# FOOTER
# ============================================================

st.divider()

st.caption(
    "ACT Operations Dashboard • "
    f"Loaded from {DATABASE}.{CONTROL_SCHEMA} • "
    f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
)
