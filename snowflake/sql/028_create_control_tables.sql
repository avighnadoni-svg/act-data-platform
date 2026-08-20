-- ============================================================================
-- ACT Data Platform - CONTROL Layer Tables
-- File: snowflake/sql/028_create_control_tables.sql
--
-- Purpose:
--   Create operational CONTROL tables for:
--
--       pipeline runs
--       study/entity loads
--       historical reprocessing
--
-- Storage-neutral design:
--
--   STORAGE_ROW_COUNT
--   STORAGE_URI
--
-- instead of cloud-provider-specific S3 columns.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ACT_DB;
USE SCHEMA CONTROL;


-- ============================================================================
-- 1. PIPELINE_RUN_AUDIT
-- ============================================================================

CREATE TABLE IF NOT EXISTS PIPELINE_RUN_AUDIT
(
    PIPELINE_AUDIT_ID       UUID
        DEFAULT UUID_STRING()
        NOT NULL,

    DAG_ID                  VARCHAR(200)
        NOT NULL,

    DAG_RUN_ID              VARCHAR(500)
        NOT NULL,

    RUN_TYPE                VARCHAR(30),

    TRIGGERED_BY            VARCHAR(200),

    STARTED_AT              TIMESTAMP_TZ,

    ENDED_AT                TIMESTAMP_TZ,

    STATUS                  VARCHAR(30),

    STUDIES_DISCOVERED      NUMBER,

    WORK_ITEMS_CREATED      NUMBER,

    SUCCESSFUL_ITEMS        NUMBER,

    FAILED_ITEMS            NUMBER,

    ERROR_MESSAGE           VARCHAR,

    CREATED_AT              TIMESTAMP_LTZ
        DEFAULT CURRENT_TIMESTAMP(),

    UPDATED_AT              TIMESTAMP_LTZ
        DEFAULT CURRENT_TIMESTAMP()
)
COMMENT =
    'One operational audit row per ACT Airflow DAG run';


-- ============================================================================
-- 2. ENTITY_LOAD_AUDIT
-- ============================================================================

CREATE TABLE IF NOT EXISTS ENTITY_LOAD_AUDIT
(
    ENTITY_LOAD_AUDIT_ID    UUID
        DEFAULT UUID_STRING()
        NOT NULL,

    DAG_ID                  VARCHAR(200),

    DAG_RUN_ID              VARCHAR(500)
        NOT NULL,

    TASK_ID                 VARCHAR(300),

    MAP_INDEX               NUMBER,

    ATTEMPT_NUMBER          NUMBER,

    STUDY_ID                VARCHAR(100)
        NOT NULL,

    ENTITY_NAME             VARCHAR(100)
        NOT NULL,

    LOAD_TYPE               VARCHAR(30)
        NOT NULL,

    STARTED_AT              TIMESTAMP_TZ,

    ENDED_AT                TIMESTAMP_TZ,

    STATUS                  VARCHAR(30),

    SOURCE_ROW_COUNT        NUMBER,

    STORAGE_ROW_COUNT       NUMBER,

    SNOWFLAKE_ROW_COUNT     NUMBER,

    SOURCE_WATERMARK_FROM   TIMESTAMP_TZ,

    SOURCE_WATERMARK_TO     TIMESTAMP_TZ,

    STORAGE_URI             VARCHAR,

    FILE_CHECKSUM           VARCHAR(128),

    ERROR_MESSAGE           VARCHAR,

    CREATED_AT              TIMESTAMP_LTZ
        DEFAULT CURRENT_TIMESTAMP(),

    UPDATED_AT              TIMESTAMP_LTZ
        DEFAULT CURRENT_TIMESTAMP()
)
COMMENT =
    'Operational audit for each ACT study/entity extraction and load attempt';


-- ============================================================================
-- 3. REPROCESS_AUDIT
-- ============================================================================

CREATE TABLE IF NOT EXISTS REPROCESS_AUDIT
(
    REPROCESS_AUDIT_ID      UUID
        DEFAULT UUID_STRING()
        NOT NULL,

    DAG_RUN_ID              VARCHAR(500),

    STUDY_ID                VARCHAR(100)
        NOT NULL,

    ENTITY_NAME             VARCHAR(100)
        NOT NULL,

    REPROCESS_FROM          TIMESTAMP_TZ
        NOT NULL,

    REPROCESS_TO            TIMESTAMP_TZ,

    REQUESTED_AT            TIMESTAMP_TZ
        DEFAULT CURRENT_TIMESTAMP(),

    REQUESTED_BY            VARCHAR(200),

    STARTED_AT              TIMESTAMP_TZ,

    ENDED_AT                TIMESTAMP_TZ,

    STATUS                  VARCHAR(30),

    SOURCE_ROW_COUNT        NUMBER,

    STORAGE_URI             VARCHAR,

    FILE_CHECKSUM           VARCHAR(128),

    NORMAL_WATERMARK_BEFORE TIMESTAMP_TZ,

    NORMAL_WATERMARK_AFTER  TIMESTAMP_TZ,

    ERROR_MESSAGE           VARCHAR,

    CREATED_AT              TIMESTAMP_LTZ
        DEFAULT CURRENT_TIMESTAMP(),

    UPDATED_AT              TIMESTAMP_LTZ
        DEFAULT CURRENT_TIMESTAMP()
)
COMMENT =
    'Audit history for ACT manual historical reprocessing requests';


-- ============================================================================
-- 4. VERIFY
-- ============================================================================

DESC TABLE PIPELINE_RUN_AUDIT;

DESC TABLE ENTITY_LOAD_AUDIT;

DESC TABLE REPROCESS_AUDIT;