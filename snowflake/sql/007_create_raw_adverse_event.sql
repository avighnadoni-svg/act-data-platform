-- ============================================================================
-- ACT Data Platform - Create RAW Adverse Event Table
-- File: snowflake/sql/007_create_raw_adverse_event.sql
--
-- Purpose:
--   Create the append-only Snowflake RAW table for normalized adverse-event
--   CSV files arriving from the ACT S3 RAW landing area.
--
-- Design:
--   - Keeps the normalized business columns exactly as delivered by the
--     act-data-platform ingestion layer.
--   - Adds Snowflake file metadata for traceability and debugging.
--   - Does NOT enforce a primary key because RAW is append-only and intentional
--     watermark overlap/replays can produce the same business record in
--     different source files/runs.
--   - Deduplication will be handled downstream, not in RAW.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE ACT_DB;
USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS RAW_ADVERSE_EVENT
(
    -- ------------------------------------------------------------------------
    -- Normalized source/business columns
    -- ------------------------------------------------------------------------
    AE_ID                   VARCHAR           COMMENT 'Adverse event business identifier',
    STUDY_ID                VARCHAR           COMMENT 'Clinical study identifier',
    SUBJECT_ID              VARCHAR           COMMENT 'Clinical subject identifier',
    EVENT_TERM              VARCHAR           COMMENT 'Reported adverse event term',
    SEVERITY                VARCHAR           COMMENT 'Normalized severity value',
    SERIOUS                 VARCHAR           COMMENT 'Source serious flag, typically Y/N',
    EVENT_DATE              DATE              COMMENT 'Adverse event date',
    REPORTED_DATE           DATE              COMMENT 'Date adverse event was reported',
    PROCESSING_PRIORITY     VARCHAR           COMMENT 'Derived ingestion/business processing priority',
    REQUIRES_SAFETY_REVIEW  BOOLEAN           COMMENT 'Derived flag indicating safety review requirement',
    UPDATED_AT              TIMESTAMP_TZ      COMMENT 'Source record watermark timestamp',

    -- ------------------------------------------------------------------------
    -- ACT ingestion audit columns written to the CSV by act-data-platform
    -- ------------------------------------------------------------------------
    SOURCE_SYSTEM           VARCHAR           COMMENT 'Source system name, e.g. RAVE_MOCK',
    SOURCE_ENTITY           VARCHAR           COMMENT 'Source entity name',
    DAG_RUN_ID              VARCHAR           COMMENT 'Airflow DAG run that produced the S3 file',
    INGESTED_AT             TIMESTAMP_TZ      COMMENT 'Timestamp when act-data-platform normalized the record',

    -- ------------------------------------------------------------------------
    -- Snowflake/S3 load metadata
    -- ------------------------------------------------------------------------
    SOURCE_FILE_NAME        VARCHAR           COMMENT 'Full staged S3 file path from METADATA$FILENAME',
    SOURCE_FILE_ROW_NUMBER  NUMBER            COMMENT 'Row number within staged file from METADATA$FILE_ROW_NUMBER',
    SOURCE_FILE_CONTENT_KEY VARCHAR           COMMENT 'Snowflake content checksum/key from METADATA$FILE_CONTENT_KEY',
    SOURCE_FILE_LAST_MODIFIED TIMESTAMP_TZ    COMMENT 'S3 file last-modified timestamp',
    SNOWFLAKE_LOAD_TS       TIMESTAMP_LTZ     COMMENT 'Snowflake scan/load timestamp from METADATA$START_SCAN_TIME'
)
COMMENT = 'Append-only RAW adverse-event records loaded from ACT normalized S3 CSV files';

-- Verify the object definition.
DESC TABLE RAW_ADVERSE_EVENT;