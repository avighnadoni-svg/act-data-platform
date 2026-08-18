-- ============================================================================
-- ACT Data Platform - VISIT Option 3 Tables
-- File: snowflake/sql/017_prepare_visit_option3.sql
--
-- Flow:
--
--   S3 visit CSV
--        |
--        v
--   LND_VISIT
--        |
--        +--> RAW_VISIT_HISTORY
--        |
--        +--> RAW_VISIT_CURRENT
--
-- Business key:
--   STUDY_ID + VISIT_ID
--
-- Change detection:
--   RECORD_HASH of business/content columns:
--     SUBJECT_ID
--     VISIT_NAME
--     PLANNED_DATE
--     ACTUAL_DATE
--
-- UPDATED_AT is intentionally excluded from RECORD_HASH so a correction can
-- still be detected when business data changes but UPDATED_AT does not.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ACT_DB;
USE SCHEMA RAW;


CREATE TRANSIENT TABLE IF NOT EXISTS LND_VISIT
(
    LND_ROW_ID                 NUMBER AUTOINCREMENT START 1 INCREMENT 1,

    VISIT_ID                   VARCHAR,
    STUDY_ID                   VARCHAR,
    SUBJECT_ID                 VARCHAR,
    VISIT_NAME                 VARCHAR,
    PLANNED_DATE               DATE,
    ACTUAL_DATE                DATE,
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
COMMENT = 'Transient landing table for ACT visit files copied from S3';


CREATE TABLE IF NOT EXISTS RAW_VISIT_HISTORY
(
    VISIT_ID                   VARCHAR,
    STUDY_ID                   VARCHAR,
    SUBJECT_ID                 VARCHAR,
    VISIT_NAME                 VARCHAR,
    PLANNED_DATE               DATE,
    ACTUAL_DATE                DATE,
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
COMMENT = 'Unique historical versions of ACT visit records';


CREATE TABLE IF NOT EXISTS RAW_VISIT_CURRENT
(
    VISIT_ID                   VARCHAR,
    STUDY_ID                   VARCHAR,
    SUBJECT_ID                 VARCHAR,
    VISIT_NAME                 VARCHAR,
    PLANNED_DATE               DATE,
    ACTUAL_DATE                DATE,
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
COMMENT = 'Current ACT visit state; one row per STUDY_ID + VISIT_ID';


DESC TABLE LND_VISIT;
DESC TABLE RAW_VISIT_HISTORY;
DESC TABLE RAW_VISIT_CURRENT;
