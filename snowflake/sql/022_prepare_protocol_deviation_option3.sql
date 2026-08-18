-- ============================================================================
-- ACT Data Platform - PROTOCOL_DEVIATION Option 3 Tables
-- File: snowflake/sql/022_prepare_protocol_deviation_option3.sql
--
-- Confirmed normalized CSV order from stage preview:
--
--   1  deviation_id
--   2  study_id
--   3  subject_id
--   4  site_id
--   5  deviation_type
--   6  severity
--   7  updated_at
--   8  source_system
--   9  source_entity
--   10 dag_run_id
--   11 ingested_at
--
-- Flow:
--
--   S3 protocol_deviation.csv
--              |
--              v
--   LND_PROTOCOL_DEVIATION
--              |
--              +--> RAW_PROTOCOL_DEVIATION_HISTORY
--              |
--              +--> RAW_PROTOCOL_DEVIATION_CURRENT
--
-- Business key:
--   STUDY_ID + DEVIATION_ID
--
-- Change detection:
--   RECORD_HASH of business/content columns:
--     SUBJECT_ID
--     SITE_ID
--     DEVIATION_TYPE
--     SEVERITY
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

CREATE TRANSIENT TABLE IF NOT EXISTS LND_PROTOCOL_DEVIATION
(
    LND_ROW_ID                 NUMBER AUTOINCREMENT START 1 INCREMENT 1,

    DEVIATION_ID               VARCHAR,
    STUDY_ID                   VARCHAR,
    SUBJECT_ID                 VARCHAR,
    SITE_ID                    VARCHAR,
    DEVIATION_TYPE             VARCHAR,
    SEVERITY                   VARCHAR,
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
COMMENT = 'Transient landing table for ACT protocol-deviation files copied from S3';


-- ============================================================================
-- 2. HISTORY
-- ============================================================================

CREATE TABLE IF NOT EXISTS RAW_PROTOCOL_DEVIATION_HISTORY
(
    DEVIATION_ID               VARCHAR,
    STUDY_ID                   VARCHAR,
    SUBJECT_ID                 VARCHAR,
    SITE_ID                    VARCHAR,
    DEVIATION_TYPE             VARCHAR,
    SEVERITY                   VARCHAR,
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
COMMENT = 'Unique historical versions of ACT protocol-deviation records';


-- ============================================================================
-- 3. CURRENT
-- ============================================================================

CREATE TABLE IF NOT EXISTS RAW_PROTOCOL_DEVIATION_CURRENT
(
    DEVIATION_ID               VARCHAR,
    STUDY_ID                   VARCHAR,
    SUBJECT_ID                 VARCHAR,
    SITE_ID                    VARCHAR,
    DEVIATION_TYPE             VARCHAR,
    SEVERITY                   VARCHAR,
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
COMMENT = 'Current ACT protocol-deviation state; one row per STUDY_ID + DEVIATION_ID';


-- ============================================================================
-- 4. VERIFY
-- ============================================================================

DESC TABLE LND_PROTOCOL_DEVIATION;
DESC TABLE RAW_PROTOCOL_DEVIATION_HISTORY;
DESC TABLE RAW_PROTOCOL_DEVIATION_CURRENT;
