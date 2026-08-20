-- ============================================================================
-- ACT Data Platform - Preview ADVERSE_EVENT Internal Stage
-- File: snowflake/sql/006_preview_adverse_event_stage.sql
--
-- Purpose:
--   Inspect normalized adverse_event CSV files after they have been uploaded
--   into the ACT Snowflake internal RAW stage.
--
-- Stage:
--
--   ACT_DB.RAW.ACT_RAW_STAGE
--
-- Normalized CSV order:
--
--   1  ae_id
--   2  study_id
--   3  subject_id
--   4  event_term
--   5  severity
--   6  serious
--   7  event_date
--   8  reported_date
--   9  processing_priority
--   10 requires_safety_review
--   11 updated_at
--   12 source_system
--   13 source_entity
--   14 dag_run_id
--   15 ingested_at
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ACT_DB;
USE SCHEMA RAW;


-- ============================================================================
-- FILES
-- ============================================================================

LIST @ACT_DB.RAW.ACT_RAW_STAGE
PATTERN = '.*adverse_event/.*/adverse_event[.]csv';


-- ============================================================================
-- PREVIEW
-- ============================================================================

SELECT
    METADATA$FILENAME
        AS SOURCE_FILE_NAME,

    METADATA$FILE_ROW_NUMBER
        AS SOURCE_FILE_ROW_NUMBER,

    t.$1::VARCHAR
        AS AE_ID,

    t.$2::VARCHAR
        AS STUDY_ID,

    t.$3::VARCHAR
        AS SUBJECT_ID,

    t.$4::VARCHAR
        AS EVENT_TERM,

    t.$5::VARCHAR
        AS SEVERITY,

    t.$6::VARCHAR
        AS SERIOUS,

    TRY_TO_DATE(
        t.$7::VARCHAR
    ) AS EVENT_DATE,

    TRY_TO_DATE(
        t.$8::VARCHAR
    ) AS REPORTED_DATE,

    t.$9::VARCHAR
        AS PROCESSING_PRIORITY,

    TRY_TO_BOOLEAN(
        t.$10::VARCHAR
    ) AS REQUIRES_SAFETY_REVIEW,

    TRY_TO_TIMESTAMP_TZ(
        t.$11::VARCHAR
    ) AS UPDATED_AT,

    t.$12::VARCHAR
        AS SOURCE_SYSTEM,

    t.$13::VARCHAR
        AS SOURCE_ENTITY,

    t.$14::VARCHAR
        AS DAG_RUN_ID,

    TRY_TO_TIMESTAMP_TZ(
        t.$15::VARCHAR
    ) AS INGESTED_AT

FROM
    @ACT_DB.RAW.ACT_RAW_STAGE
(
    PATTERN =>
        '.*adverse_event/.*/adverse_event[.]csv'
) t

ORDER BY
    SOURCE_FILE_NAME,
    SOURCE_FILE_ROW_NUMBER

LIMIT 50;