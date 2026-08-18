-- ============================================================================
-- ACT Data Platform - SITE Option 3 Tables
-- File: snowflake/sql/013_prepare_site_option3.sql
--
-- Flow:
--
--   S3 site CSV
--        |
--        v
--   LND_SITE
--        |
--        +--> RAW_SITE_HISTORY
--        |
--        +--> RAW_SITE_CURRENT
--
-- Business key:
--   STUDY_ID + SITE_ID
--
-- Change detection:
--   RECORD_HASH of business/content columns:
--     COUNTRY
--     INVESTIGATOR
--     TARGET_ENROLLMENT
--
-- UPDATED_AT is intentionally excluded from RECORD_HASH so a correction can
-- still be detected when the client changes data without changing UPDATED_AT.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ACT_DB;
USE SCHEMA RAW;

CREATE TRANSIENT TABLE IF NOT EXISTS LND_SITE
(
    LND_ROW_ID                 NUMBER AUTOINCREMENT START 1 INCREMENT 1,

    SITE_ID                    VARCHAR,
    STUDY_ID                   VARCHAR,
    COUNTRY                    VARCHAR,
    INVESTIGATOR               VARCHAR,
    TARGET_ENROLLMENT          NUMBER,
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
COMMENT = 'Transient landing table for ACT site files copied from S3';


CREATE TABLE IF NOT EXISTS RAW_SITE_HISTORY
(
    SITE_ID                    VARCHAR,
    STUDY_ID                   VARCHAR,
    COUNTRY                    VARCHAR,
    INVESTIGATOR               VARCHAR,
    TARGET_ENROLLMENT          NUMBER,
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
COMMENT = 'Unique historical versions of ACT site records';


CREATE TABLE IF NOT EXISTS RAW_SITE_CURRENT
(
    SITE_ID                    VARCHAR,
    STUDY_ID                   VARCHAR,
    COUNTRY                    VARCHAR,
    INVESTIGATOR               VARCHAR,
    TARGET_ENROLLMENT          NUMBER,
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
COMMENT = 'Current ACT site state; one row per STUDY_ID + SITE_ID';


DESC TABLE LND_SITE;
DESC TABLE RAW_SITE_HISTORY;
DESC TABLE RAW_SITE_CURRENT;
