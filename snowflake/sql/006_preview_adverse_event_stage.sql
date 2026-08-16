-- ============================================================================
-- ACT Data Platform - Preview Adverse Event Files in RAW External Stage
-- File: snowflake/sql/006_preview_adverse_event_stage.sql
--
-- Purpose:
--   Inspect normalized adverse_event CSV files in S3 before loading them into
--   ACT_DB.RAW.RAW_ADVERSE_EVENT.
--
-- Stage location:
--   ACT_DB.RAW.ACT_RAW_S3_STAGE
--
-- Expected CSV column order produced by the current ACT normalizer:
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
USE DATABASE ACT_DB;
USE SCHEMA RAW;

-- Show adverse_event files across all studies, dates, and Airflow runs.
LIST @ACT_RAW_S3_STAGE
    PATTERN = '.*adverse_event/.*/adverse_event[.]csv';

-- Preview staged rows plus Snowflake file metadata.
SELECT
    METADATA$FILENAME AS source_file_name,
    METADATA$FILE_ROW_NUMBER AS source_file_row_number,

    t.$1::VARCHAR  AS ae_id,
    t.$2::VARCHAR  AS study_id,
    t.$3::VARCHAR  AS subject_id,
    t.$4::VARCHAR  AS event_term,
    t.$5::VARCHAR  AS severity,
    t.$6::VARCHAR  AS serious,
    TRY_TO_DATE(t.$7::VARCHAR) AS event_date,
    TRY_TO_DATE(t.$8::VARCHAR) AS reported_date,
    t.$9::VARCHAR  AS processing_priority,
    TRY_TO_BOOLEAN(t.$10::VARCHAR) AS requires_safety_review,
    TRY_TO_TIMESTAMP_TZ(t.$11::VARCHAR) AS updated_at,
    t.$12::VARCHAR AS source_system,
    t.$13::VARCHAR AS source_entity,
    t.$14::VARCHAR AS dag_run_id,
    TRY_TO_TIMESTAMP_TZ(t.$15::VARCHAR) AS ingested_at

FROM @ACT_RAW_S3_STAGE (
    PATTERN => '.*adverse_event/.*/adverse_event[.]csv'
) t
ORDER BY
    source_file_name,
    source_file_row_number
LIMIT 50;
