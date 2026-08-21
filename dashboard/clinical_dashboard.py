from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import snowflake.connector
import streamlit as st

DEFAULT_CONNECTION_NAME = "SNOWFLAKE_ACT_DEV"
DATABASE = os.getenv("ACT_SNOWFLAKE_DATABASE", "ACT_DB")
MART_SCHEMA = os.getenv("ACT_DBT_MART_SCHEMA", "DBT_DEV_MARTS")

STUDY_OVERVIEW_TABLE = f"{DATABASE}.{MART_SCHEMA}.MART_STUDY_OVERVIEW"
SITE_PERFORMANCE_TABLE = f"{DATABASE}.{MART_SCHEMA}.MART_SITE_PERFORMANCE"
SAFETY_OVERVIEW_TABLE = f"{DATABASE}.{MART_SCHEMA}.MART_SAFETY_OVERVIEW"
DATA_QUALITY_TABLE = f"{DATABASE}.{MART_SCHEMA}.MART_DATA_QUALITY_OVERVIEW"

st.set_page_config(page_title="ACT Clinical Dashboard", page_icon="🧬", layout="wide")
st.title("ACT Clinical Trial Dashboard")
st.caption(
    "Study progress, site performance, safety operations and data-quality monitoring from Snowflake/dbt marts."
)
st.info(
    "This dashboard supports operational monitoring and review. It is not a clinical diagnosis or medical decision system."
)


def get_connection_name() -> str:
    return os.getenv("SNOWFLAKE_CONNECTION_NAME") or DEFAULT_CONNECTION_NAME


def execute_dataframe(cursor: Any, sql: str) -> pd.DataFrame:
    cursor.execute(sql)
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    return pd.DataFrame(rows, columns=columns)


@st.cache_data(ttl=60, show_spinner=False)
def load_clinical_data() -> dict[str, pd.DataFrame]:
    connection = snowflake.connector.connect(
        connection_name=get_connection_name(),
        application="ACT_STREAMLIT_CLINICAL_DASHBOARD",
    )
    try:
        cursor = connection.cursor()
        try:
            study = execute_dataframe(
                cursor,
                f"SELECT * FROM {STUDY_OVERVIEW_TABLE} ORDER BY STUDY_ID",
            )
            site = execute_dataframe(
                cursor,
                f"SELECT * FROM {SITE_PERFORMANCE_TABLE} ORDER BY STUDY_ID, SITE_ID",
            )
            safety = execute_dataframe(
                cursor,
                f"SELECT * FROM {SAFETY_OVERVIEW_TABLE} ORDER BY STUDY_ID, SITE_ID, SUBJECT_ID",
            )
            quality = execute_dataframe(
                cursor,
                f"SELECT * FROM {DATA_QUALITY_TABLE} ORDER BY STUDY_ID, SITE_ID",
            )
        finally:
            cursor.close()
    finally:
        connection.close()

    return {
        "study": study,
        "site": site,
        "safety": safety,
        "quality": quality,
    }


def safe_number(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        if pd.isna(value):
            return default
    except TypeError:
        pass
    return float(value)


def safe_int(value: Any) -> int:
    return int(round(safe_number(value)))


def weighted_enrollment_pct(enrolled: pd.Series, target: pd.Series) -> float:
    enrolled_total = pd.to_numeric(enrolled, errors="coerce").fillna(0).sum()
    target_total = pd.to_numeric(target, errors="coerce").fillna(0).sum()
    if target_total <= 0:
        return 0.0
    return float(enrolled_total) / float(target_total) * 100.0


def apply_filters(
    dataframe: pd.DataFrame,
    selected_studies: list[str],
    selected_countries: list[str] | None = None,
) -> pd.DataFrame:
    result = dataframe.copy()
    if selected_studies:
        result = result[result["STUDY_ID"].isin(selected_studies)]
    if selected_countries is not None and selected_countries and "COUNTRY" in result.columns:
        result = result[result["COUNTRY"].isin(selected_countries)]
    return result


with st.spinner("Loading clinical marts from Snowflake..."):
    try:
        data = load_clinical_data()
    except Exception as exc:
        st.error("Unable to load ACT clinical marts from Snowflake.")
        st.exception(exc)
        st.stop()

study_df = data["study"]
site_df = data["site"]
safety_df = data["safety"]
quality_df = data["quality"]

all_studies = sorted(study_df["STUDY_ID"].dropna().astype(str).unique().tolist())
all_countries = sorted(site_df["COUNTRY"].dropna().astype(str).unique().tolist())

with st.sidebar:
    st.header("Clinical Filters")
    selected_studies = st.multiselect("Study", options=all_studies, default=all_studies)
    selected_countries = st.multiselect(
        "Country",
        options=all_countries,
        default=all_countries,
        help=(
            "Country filtering applies to Site Performance, Safety and Data Quality. "
            "Study Overview remains study-level."
        ),
    )
    st.divider()
    st.write("Snowflake connection:")
    st.code(get_connection_name(), language=None)
    if st.button("Refresh now", width="stretch"):
        st.cache_data.clear()
        st.rerun()
    st.caption("Clinical mart data is cached for 60 seconds.")

filtered_study = apply_filters(study_df, selected_studies)
filtered_site = apply_filters(site_df, selected_studies, selected_countries)
filtered_safety = apply_filters(safety_df, selected_studies, selected_countries)
filtered_quality = apply_filters(quality_df, selected_studies, selected_countries)

st.subheader("Executive Study Overview")

if filtered_study.empty:
    st.warning("No study data matches the selected filters.")
else:
    total_studies = filtered_study["STUDY_ID"].nunique()
    total_sites = safe_int(filtered_study["SITE_COUNT"].sum())
    total_subjects = safe_int(filtered_study["SUBJECT_COUNT"].sum())
    enrolled_subjects = safe_int(filtered_study["ENROLLED_SUBJECT_COUNT"].sum())
    enrollment_pct = weighted_enrollment_pct(
        filtered_study["ENROLLED_SUBJECT_COUNT"],
        filtered_study["TARGET_SUBJECTS"],
    )
    open_queries = safe_int(filtered_study["OPEN_QUERY_COUNT"].sum())

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Studies", total_studies)
    k2.metric("Sites", total_sites)
    k3.metric("Subjects", total_subjects)
    k4.metric("Enrolled", enrolled_subjects)
    k5.metric("Enrollment", f"{enrollment_pct:,.1f}%")
    k6.metric("Open Queries", open_queries)


tab_study, tab_site, tab_safety, tab_quality = st.tabs(
    ["Study Overview", "Site Performance", "Safety", "Data Quality"]
)

with tab_study:
    st.subheader("Study Progress")
    if filtered_study.empty:
        st.info("No study rows available.")
    else:
        study_display_columns = [
            "STUDY_ID",
            "STUDY_NAME",
            "PHASE",
            "TARGET_SUBJECTS",
            "SITE_COUNT",
            "SUBJECT_COUNT",
            "ENROLLED_SUBJECT_COUNT",
            "ENROLLMENT_GAP",
            "ENROLLMENT_PCT",
            "VISIT_COUNT",
            "COMPLETED_VISIT_COUNT",
            "PENDING_VISIT_COUNT",
            "ADVERSE_EVENT_COUNT",
            "SERIOUS_AE_COUNT",
            "OPEN_QUERY_COUNT",
        ]
        st.dataframe(filtered_study[study_display_columns], width="stretch", hide_index=True)

        enrollment_chart = filtered_study[
            ["STUDY_ID", "TARGET_SUBJECTS", "ENROLLED_SUBJECT_COUNT"]
        ].set_index("STUDY_ID")
        st.caption("Target subjects vs enrolled subjects")
        st.bar_chart(enrollment_chart)

        visit_chart = filtered_study[
            ["STUDY_ID", "COMPLETED_VISIT_COUNT", "PENDING_VISIT_COUNT"]
        ].set_index("STUDY_ID")
        st.caption("Completed vs pending visits")
        st.bar_chart(visit_chart)

with tab_site:
    st.subheader("Site Enrollment Performance")
    if filtered_site.empty:
        st.info("No site rows match the selected filters.")
    else:
        site_count = filtered_site["SITE_ID"].nunique()
        site_enrollment_pct = weighted_enrollment_pct(
            filtered_site["ENROLLED_SUBJECT_COUNT"],
            filtered_site["TARGET_ENROLLMENT"],
        )
        sites_with_open_queries = safe_int(
            (pd.to_numeric(filtered_site["OPEN_QUERY_COUNT"], errors="coerce").fillna(0) > 0).sum()
        )
        sites_with_serious_ae = safe_int(
            (pd.to_numeric(filtered_site["SERIOUS_AE_COUNT"], errors="coerce").fillna(0) > 0).sum()
        )

        s1, s2, s3, s4 = st.columns(4)
        s1.metric("Sites in Scope", site_count)
        s2.metric("Site Enrollment", f"{site_enrollment_pct:,.1f}%")
        s3.metric("Sites with Open Queries", sites_with_open_queries)
        s4.metric("Sites with Serious AEs", sites_with_serious_ae)

        site_display_columns = [
            "STUDY_ID",
            "SITE_ID",
            "COUNTRY",
            "INVESTIGATOR",
            "TARGET_ENROLLMENT",
            "SUBJECT_COUNT",
            "ENROLLED_SUBJECT_COUNT",
            "ENROLLMENT_GAP",
            "ENROLLMENT_PCT",
            "ADVERSE_EVENT_COUNT",
            "SERIOUS_AE_COUNT",
            "OPEN_QUERY_COUNT",
            "PROTOCOL_DEVIATION_COUNT",
        ]
        site_display = filtered_site[site_display_columns].sort_values(
            ["ENROLLMENT_PCT", "STUDY_ID", "SITE_ID"],
            ascending=[True, True, True],
        )
        st.dataframe(site_display, width="stretch", hide_index=True)

        site_chart = filtered_site[["SITE_ID", "ENROLLMENT_PCT"]].set_index("SITE_ID")
        st.caption("Enrollment percentage by site")
        st.bar_chart(site_chart)

        country_summary = filtered_site.groupby("COUNTRY", dropna=False).agg(
            SITE_COUNT=("SITE_ID", "nunique"),
            TARGET_ENROLLMENT=("TARGET_ENROLLMENT", "sum"),
            ENROLLED_SUBJECTS=("ENROLLED_SUBJECT_COUNT", "sum"),
            OPEN_QUERIES=("OPEN_QUERY_COUNT", "sum"),
        ).reset_index()
        country_summary["ENROLLMENT_PCT"] = country_summary.apply(
            lambda row: (
                safe_number(row["ENROLLED_SUBJECTS"])
                / safe_number(row["TARGET_ENROLLMENT"], default=1.0)
                * 100.0
            ) if safe_number(row["TARGET_ENROLLMENT"]) > 0 else 0.0,
            axis=1,
        )
        st.subheader("Country Summary")
        st.dataframe(country_summary, width="stretch", hide_index=True)

with tab_safety:
    st.subheader("Safety Operations")
    if filtered_safety.empty:
        st.info("No safety rows match the selected filters.")
    else:
        total_ae = safe_int(filtered_safety["ADVERSE_EVENT_COUNT"].sum())
        serious_ae = safe_int(filtered_safety["SERIOUS_AE_COUNT"].sum())
        severe_ae = safe_int(filtered_safety["SEVERE_AE_COUNT"].sum())
        safety_review_ae = safe_int(filtered_safety["SAFETY_REVIEW_AE_COUNT"].sum())
        attention_subjects = safe_int(
            filtered_safety["REQUIRES_SAFETY_ATTENTION"].fillna(False).astype(bool).sum()
        )
        average_reporting_delay = pd.to_numeric(
            filtered_safety["AVG_AE_REPORTING_DELAY_DAYS"], errors="coerce"
        ).dropna().mean()

        a1, a2, a3, a4, a5 = st.columns(5)
        a1.metric("Adverse Events", total_ae)
        a2.metric("Serious AEs", serious_ae)
        a3.metric("Severe AEs", severe_ae)
        a4.metric("Safety Review AEs", safety_review_ae)
        a5.metric("Subjects Requiring Attention", attention_subjects)

        if pd.notna(average_reporting_delay):
            st.caption(f"Average subject-level AE reporting delay: {average_reporting_delay:,.1f} days")

        attention_only = filtered_safety[
            filtered_safety["REQUIRES_SAFETY_ATTENTION"].fillna(False).astype(bool)
        ].copy()

        st.subheader("Subjects Requiring Safety Attention")
        if attention_only.empty:
            st.success("No subjects currently meet the operational safety-attention rule.")
        else:
            safety_display_columns = [
                "STUDY_ID",
                "SITE_ID",
                "SUBJECT_ID",
                "COUNTRY",
                "SUBJECT_STATUS",
                "ADVERSE_EVENT_COUNT",
                "SERIOUS_AE_COUNT",
                "SEVERE_AE_COUNT",
                "SAFETY_REVIEW_AE_COUNT",
                "AVG_AE_REPORTING_DELAY_DAYS",
                "MAX_AE_REPORTING_DELAY_DAYS",
                "ABNORMAL_LAB_COUNT",
                "HAS_SERIOUS_AE",
                "HAS_SEVERE_AE",
                "HAS_ABNORMAL_LAB",
            ]
            st.dataframe(attention_only[safety_display_columns], width="stretch", hide_index=True)

        safety_by_study = filtered_safety.groupby("STUDY_ID", dropna=False).agg(
            ADVERSE_EVENTS=("ADVERSE_EVENT_COUNT", "sum"),
            SERIOUS_AES=("SERIOUS_AE_COUNT", "sum"),
            SEVERE_AES=("SEVERE_AE_COUNT", "sum"),
            SAFETY_REVIEW_AES=("SAFETY_REVIEW_AE_COUNT", "sum"),
        )
        st.caption("Adverse-event operational summary by study")
        st.bar_chart(safety_by_study)

with tab_quality:
    st.subheader("Data Quality Monitoring")
    if filtered_quality.empty:
        st.info("No data-quality rows match the selected filters.")
    else:
        total_queries = safe_int(filtered_quality["DATA_QUERY_COUNT"].sum())
        open_queries = safe_int(filtered_quality["OPEN_QUERY_COUNT"].sum())
        resolved_queries = safe_int(filtered_quality["RESOLVED_QUERY_COUNT"].sum())
        total_deviations = safe_int(filtered_quality["PROTOCOL_DEVIATION_COUNT"].sum())
        critical_deviations = safe_int(filtered_quality["CRITICAL_DEVIATION_COUNT"].sum())
        attention_sites = safe_int(
            filtered_quality["REQUIRES_DATA_QUALITY_ATTENTION"].fillna(False).astype(bool).sum()
        )

        q1, q2, q3, q4, q5, q6 = st.columns(6)
        q1.metric("Data Queries", total_queries)
        q2.metric("Open Queries", open_queries)
        q3.metric("Resolved Queries", resolved_queries)
        q4.metric("Protocol Deviations", total_deviations)
        q5.metric("Critical Deviations", critical_deviations)
        q6.metric("Sites Requiring Attention", attention_sites)

        quality_attention = filtered_quality[
            filtered_quality["REQUIRES_DATA_QUALITY_ATTENTION"].fillna(False).astype(bool)
        ].copy()

        st.subheader("Sites Requiring Data-Quality Attention")
        if quality_attention.empty:
            st.success("No sites currently meet the data-quality attention rule.")
        else:
            quality_display_columns = [
                "STUDY_ID",
                "SITE_ID",
                "COUNTRY",
                "INVESTIGATOR",
                "DATA_QUERY_COUNT",
                "OPEN_QUERY_COUNT",
                "RESOLVED_QUERY_COUNT",
                "SUBJECTS_WITH_OPEN_QUERIES",
                "AVG_QUERY_RESOLUTION_DAYS",
                "MAX_QUERY_RESOLUTION_DAYS",
                "OLDEST_OPEN_QUERY_DAYS",
                "PROTOCOL_DEVIATION_COUNT",
                "MAJOR_DEVIATION_COUNT",
                "CRITICAL_DEVIATION_COUNT",
                "HAS_OPEN_QUERIES",
                "HAS_SIGNIFICANT_DEVIATIONS",
            ]
            st.dataframe(quality_attention[quality_display_columns], width="stretch", hide_index=True)

        quality_by_site = filtered_quality[
            ["SITE_ID", "OPEN_QUERY_COUNT", "MAJOR_DEVIATION_COUNT", "CRITICAL_DEVIATION_COUNT"]
        ].set_index("SITE_ID")
        st.caption("Open queries and significant deviations by site")
        st.bar_chart(quality_by_site)

st.divider()
st.caption(
    "ACT Clinical Trial Dashboard • "
    f"Source: {DATABASE}.{MART_SCHEMA} • "
    f"Refreshed {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
)
