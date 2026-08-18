-- ============================================================================
-- ACT Data Platform - Snowflake Watermark Single Source of Truth
-- File: snowflake/sql/030_create_watermark_table.sql
--
-- Purpose:
--   Store the ACTIVE incremental extraction watermark in Snowflake.
--
-- Single source of truth:
--
--   ACT_DB.CONTROL.WATERMARK
--
-- Grain:
--
--   STUDY_ID + ENTITY_NAME
--
-- Important:
--   Airflow Variables can be retained temporarily for rollback/migration
--   validation, but the replacement WatermarkManager will no longer read or
--   write them.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ACT_DB;
USE SCHEMA CONTROL;


-- ============================================================================
-- 1. CREATE WATERMARK TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS ACT_DB.CONTROL.WATERMARK
(
    STUDY_ID                VARCHAR(100) NOT NULL,
    ENTITY_NAME             VARCHAR(100) NOT NULL,

    WATERMARK_VALUE         TIMESTAMP_TZ NOT NULL,

    LAST_SUCCESSFUL_RUN_ID  VARCHAR(500),

    CREATED_AT              TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT              TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_ACT_WATERMARK
        PRIMARY KEY (STUDY_ID, ENTITY_NAME)
)
COMMENT = 'Single source of truth for ACT incremental extraction watermarks';


-- ============================================================================
-- 2. VERIFY TABLE
-- ============================================================================

DESC TABLE ACT_DB.CONTROL.WATERMARK;


-- ============================================================================
-- 3. CURRENT WATERMARK ROWS
--
-- On first creation this can be empty.
-- Existing Airflow watermarks are migrated by:
--
--   scripts/migrate_airflow_watermarks_to_snowflake.py
-- ============================================================================

SELECT
    STUDY_ID,
    ENTITY_NAME,
    WATERMARK_VALUE,
    LAST_SUCCESSFUL_RUN_ID,
    CREATED_AT,
    UPDATED_AT
FROM ACT_DB.CONTROL.WATERMARK
ORDER BY
    STUDY_ID,
    ENTITY_NAME;


-- ============================================================================
-- 4. DUPLICATE CHECK
--
-- Must return zero rows.
-- Snowflake standard-table PK constraints are informational, so we also
-- validate the business-key grain explicitly.
-- ============================================================================

SELECT
    STUDY_ID,
    ENTITY_NAME,
    COUNT(*) AS ROW_COUNT
FROM ACT_DB.CONTROL.WATERMARK
GROUP BY
    STUDY_ID,
    ENTITY_NAME
HAVING COUNT(*) > 1;
