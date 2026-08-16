-- ============================================================================
-- ACT Data Platform - Load RAW Adverse Event
-- File: snowflake/sql/008_load_raw_adverse_event.sql
--
-- Purpose:
--   Load all adverse_event CSV files visible through ACT_DB.RAW.ACT_RAW_S3_STAGE
--   into ACT_DB.RAW.RAW_ADVERSE_EVENT.
--
-- Design:
--   - Loads both ONC101 and ONC102.
--   - Uses Snowflake file-load history to avoid reloading the same file
--     during normal reruns.
--   - FORCE is intentionally FALSE.
--   - ON_ERROR = ABORT_STATEMENT so malformed input fails visibly.
--   - Loads Snowflake staged-file metadata for traceability.
--   - RAW remains append-only; business-key deduplication is deferred to
--     downstream transformation layers.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ACT_DB;
USE SCHEMA RAW;

-- ---------------------------------------------------------------------------
-- 1. Ensure the target table exists before attempting COPY
-- ---------------------------------------------------------------------------

SHOW TABLES LIKE 'RAW_ADVERSE_EVENT' IN SCHEMA ACT_DB.RAW;

-- ---------------------------------------------------------------------------
-- 2. Load new adverse_event files from S3
-- ---------------------------------------------------------------------------

COPY INTO ACT_DB.RAW.RAW_ADVERSE_EVENT
(
    AE_ID,
    STUDY_ID,
    SUBJECT_ID,
    EVENT_TERM,
    SEVERITY,
    SERIOUS,
    EVENT_DATE,
    REPORTED_DATE,
    PROCESSING_PRIORITY,
    REQUIRES_SAFETY_REVIEW,
    UPDATED_AT,
    SOURCE_SYSTEM,
    SOURCE_ENTITY,
    DAG_RUN_ID,
    INGESTED_AT,
    SOURCE_FILE_NAME,
    SOURCE_FILE_ROW_NUMBER,
    SOURCE_FILE_CONTENT_KEY,
    SOURCE_FILE_LAST_MODIFIED,
    SNOWFLAKE_LOAD_TS
)
FROM
(
    SELECT
        t.$1::VARCHAR,
        t.$2::VARCHAR,
        t.$3::VARCHAR,
        t.$4::VARCHAR,
        t.$5::VARCHAR,
        t.$6::VARCHAR,
        TRY_TO_DATE(t.$7::VARCHAR),
        TRY_TO_DATE(t.$8::VARCHAR),
        t.$9::VARCHAR,
        TRY_TO_BOOLEAN(t.$10::VARCHAR),
        TRY_TO_TIMESTAMP_TZ(t.$11::VARCHAR),
        t.$12::VARCHAR,
        t.$13::VARCHAR,
        t.$14::VARCHAR,
        TRY_TO_TIMESTAMP_TZ(t.$15::VARCHAR),

        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        METADATA$FILE_CONTENT_KEY,
        METADATA$FILE_LAST_MODIFIED,
        METADATA$START_SCAN_TIME

    FROM @ACT_DB.RAW.ACT_RAW_S3_STAGE
    (
        PATTERN => '.*adverse_event/.*/adverse_event[.]csv'
    ) t
)
ON_ERROR = 'ABORT_STATEMENT'
FORCE = FALSE;

-- ---------------------------------------------------------------------------
-- 3. Verify total RAW rows by study
-- ---------------------------------------------------------------------------

SELECT
    STUDY_ID,
    COUNT(*) AS RAW_ROW_COUNT,
    COUNT(DISTINCT AE_ID) AS DISTINCT_AE_COUNT,
    COUNT(DISTINCT SOURCE_FILE_NAME) AS SOURCE_FILE_COUNT,
    MIN(UPDATED_AT) AS MIN_SOURCE_UPDATED_AT,
    MAX(UPDATED_AT) AS MAX_SOURCE_UPDATED_AT,
    MAX(SNOWFLAKE_LOAD_TS) AS LATEST_SNOWFLAKE_LOAD_TS
FROM ACT_DB.RAW.RAW_ADVERSE_EVENT
GROUP BY STUDY_ID
ORDER BY STUDY_ID;

-- ---------------------------------------------------------------------------
-- 4. Inspect recent rows and traceability metadata
-- ---------------------------------------------------------------------------

SELECT
    STUDY_ID,
    AE_ID,
    EVENT_TERM,
    SEVERITY,
    UPDATED_AT,
    DAG_RUN_ID,
    SOURCE_FILE_NAME,
    SOURCE_FILE_ROW_NUMBER,
    SOURCE_FILE_CONTENT_KEY,
    SOURCE_FILE_LAST_MODIFIED,
    SNOWFLAKE_LOAD_TS
FROM ACT_DB.RAW.RAW_ADVERSE_EVENT
ORDER BY
    SNOWFLAKE_LOAD_TS DESC,
    SOURCE_FILE_NAME,
    SOURCE_FILE_ROW_NUMBER
LIMIT 50;

-- ---------------------------------------------------------------------------
-- 5. Identify intentional business-record replays/overlap in RAW
--
-- A count > 1 here does NOT automatically mean COPY duplicated the same file.
-- Our upstream watermark overlap can legitimately produce the same
-- AE_ID + UPDATED_AT in different Airflow/S3 run files.
-- ---------------------------------------------------------------------------

SELECT
    STUDY_ID,
    AE_ID,
    UPDATED_AT,
    COUNT(*) AS RAW_OCCURRENCES,
    COUNT(DISTINCT SOURCE_FILE_NAME) AS FILE_COUNT
FROM ACT_DB.RAW.RAW_ADVERSE_EVENT
GROUP BY
    STUDY_ID,
    AE_ID,
    UPDATED_AT
HAVING COUNT(*) > 1
ORDER BY
    RAW_OCCURRENCES DESC,
    STUDY_ID,
    AE_ID;

-- ---------------------------------------------------------------------------
-- 6. Snowflake COPY load history for this RAW table
-- ---------------------------------------------------------------------------

SELECT
    FILE_NAME,
    STATUS,
    ROW_COUNT,
    ROW_PARSED,
    ERROR_COUNT,
    FIRST_ERROR_MESSAGE,
    LAST_LOAD_TIME
FROM ACT_DB.INFORMATION_SCHEMA.LOAD_HISTORY
WHERE
    SCHEMA_NAME = 'RAW'
    AND TABLE_NAME = 'RAW_ADVERSE_EVENT'
ORDER BY LAST_LOAD_TIME DESC;
