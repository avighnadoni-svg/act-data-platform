-- ============================================================================
-- ACT Data Platform - STUDY Option 3 Tables
-- File: snowflake/sql/011_prepare_study_option3.sql
--
-- Flow:
--
--   S3 study CSV
--        |
--        v
--   LND_STUDY
--        |
--        +--> RAW_STUDY_HISTORY
--        |
--        +--> RAW_STUDY_CURRENT
--
-- Business key:
--   STUDY_ID
--
-- Change detection:
--   RECORD_HASH of business/content columns.
--
-- UPDATED_AT is intentionally excluded from RECORD_HASH so a correction can
-- still be detected when the client changes data but leaves UPDATED_AT intact.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ACT_DB;
USE SCHEMA RAW;

CREATE TRANSIENT TABLE IF NOT EXISTS LND_STUDY
(
    LND_ROW_ID                 NUMBER AUTOINCREMENT START 1 INCREMENT 1,

    STUDY_ID                   VARCHAR,
    STUDY_NAME                 VARCHAR,
    PHASE                      VARCHAR,
    TARGET_SUBJECTS            NUMBER,
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
COMMENT = 'Transient landing table for ACT study files copied from S3';


CREATE TABLE IF NOT EXISTS RAW_STUDY_HISTORY
(
    STUDY_ID                   VARCHAR,
    STUDY_NAME                 VARCHAR,
    PHASE                      VARCHAR,
    TARGET_SUBJECTS            NUMBER,
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
COMMENT = 'Unique historical versions of ACT study records';


CREATE TABLE IF NOT EXISTS RAW_STUDY_CURRENT
(
    STUDY_ID                   VARCHAR,
    STUDY_NAME                 VARCHAR,
    PHASE                      VARCHAR,
    TARGET_SUBJECTS            NUMBER,
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
COMMENT = 'Current ACT study state; one row per STUDY_ID';


DESC TABLE LND_STUDY;
DESC TABLE RAW_STUDY_HISTORY;
DESC TABLE RAW_STUDY_CURRENT;
