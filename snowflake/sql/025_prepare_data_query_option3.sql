-- ============================================================================
-- ACT Data Platform - DATA_QUERY Option 3 Tables
-- File: snowflake/sql/025_prepare_data_query_option3.sql
--
-- Confirmed normalized CSV order from stage preview:
--
--   1  query_id
--   2  study_id
--   3  subject_id
--   4  site_id
--   5  opened_date
--   6  resolved_date
--   7  status
--   8  updated_at
--   9  source_system
--   10 source_entity
--   11 dag_run_id
--   12 ingested_at
--
-- Flow:
--
--   S3 data_query.csv
--          |
--          v
--   LND_DATA_QUERY
--          |
--          +--> RAW_DATA_QUERY_HISTORY
--          |
--          +--> RAW_DATA_QUERY_CURRENT
--
-- Business key:
--   STUDY_ID + QUERY_ID
--
-- Change detection:
--   RECORD_HASH of business/content columns:
--     SUBJECT_ID
--     SITE_ID
--     OPENED_DATE
--     RESOLVED_DATE
--     STATUS
--
-- UPDATED_AT is intentionally excluded from RECORD_HASH so a correction can
-- still be detected when business data changes but UPDATED_AT does not.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ACT_DB;
USE SCHEMA RAW;


-- ============================================================================
-- 1. LANDING
-- ============================================================================

CREATE TRANSIENT TABLE IF NOT EXISTS LND_DATA_QUERY
(
    LND_ROW_ID                 NUMBER AUTOINCREMENT START 1 INCREMENT 1,

    QUERY_ID                   VARCHAR,
    STUDY_ID                   VARCHAR,
    SUBJECT_ID                 VARCHAR,
    SITE_ID                    VARCHAR,
    OPENED_DATE                DATE,
    RESOLVED_DATE              DATE,
    STATUS                     VARCHAR,
    UPDATED_AT                 TIMESTAMP_TZ,

    SOURCE_SYSTEM              VARCHAR,
    SOURCE_ENTITY              VARCHAR,
    DAG_RUN_ID                 VARCHAR,
    INGESTED_AT                TIMESTAMP_TZ,

    SOURCE_FILE_NAME           VARCHAR,
    SOURCE_FILE_ROW_NUMBER     NUMBER,
    SOURCE_FILE_CONTENT_KEY    VARCHAR,
    SOURCE_FILE_LAST_MODIFIED  TIMESTAMP_TZ,
    SNOWFLAKE_LOAD_TS          TIMESTAMP_LTZ,

    RECORD_HASH                VARCHAR(64),

    PROCESSED_FLAG             BOOLEAN DEFAULT FALSE,
    PROCESSED_AT               TIMESTAMP_LTZ
)
COMMENT = 'Transient landing table for ACT data-query files copied from S3';


-- ============================================================================
-- 2. HISTORY
-- ============================================================================

CREATE TABLE IF NOT EXISTS RAW_DATA_QUERY_HISTORY
(
    QUERY_ID                   VARCHAR,
    STUDY_ID                   VARCHAR,
    SUBJECT_ID                 VARCHAR,
    SITE_ID                    VARCHAR,
    OPENED_DATE                DATE,
    RESOLVED_DATE              DATE,
    STATUS                     VARCHAR,
    UPDATED_AT                 TIMESTAMP_TZ,

    RECORD_HASH                VARCHAR(64),

    SOURCE_SYSTEM              VARCHAR,
    SOURCE_ENTITY              VARCHAR,
    DAG_RUN_ID                 VARCHAR,
    INGESTED_AT                TIMESTAMP_TZ,

    SOURCE_FILE_NAME           VARCHAR,
    SOURCE_FILE_ROW_NUMBER     NUMBER,
    SOURCE_FILE_CONTENT_KEY    VARCHAR,
    SOURCE_FILE_LAST_MODIFIED  TIMESTAMP_TZ,
    SNOWFLAKE_LOAD_TS          TIMESTAMP_LTZ,

    HISTORY_CREATED_AT         TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Unique historical versions of ACT data-query records';


-- ============================================================================
-- 3. CURRENT
-- ============================================================================

CREATE TABLE IF NOT EXISTS RAW_DATA_QUERY_CURRENT
(
    QUERY_ID                   VARCHAR,
    STUDY_ID                   VARCHAR,
    SUBJECT_ID                 VARCHAR,
    SITE_ID                    VARCHAR,
    OPENED_DATE                DATE,
    RESOLVED_DATE              DATE,
    STATUS                     VARCHAR,
    UPDATED_AT                 TIMESTAMP_TZ,

    RECORD_HASH                VARCHAR(64),

    SOURCE_SYSTEM              VARCHAR,
    SOURCE_ENTITY              VARCHAR,
    DAG_RUN_ID                 VARCHAR,
    INGESTED_AT                TIMESTAMP_TZ,

    SOURCE_FILE_NAME           VARCHAR,
    SOURCE_FILE_ROW_NUMBER     NUMBER,
    SOURCE_FILE_CONTENT_KEY    VARCHAR,
    SOURCE_FILE_LAST_MODIFIED  TIMESTAMP_TZ,
    SNOWFLAKE_LOAD_TS          TIMESTAMP_LTZ,

    CURRENT_ROW_UPDATED_AT     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Current ACT data-query state; one row per STUDY_ID + QUERY_ID';


-- ============================================================================
-- 4. VERIFY
-- ============================================================================

DESC TABLE LND_DATA_QUERY;
DESC TABLE RAW_DATA_QUERY_HISTORY;
DESC TABLE RAW_DATA_QUERY_CURRENT;
