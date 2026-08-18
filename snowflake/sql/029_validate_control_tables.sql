-- ============================================================================
-- ACT Data Platform - Validate CONTROL Layer
-- File: snowflake/sql/029_validate_control_tables.sql
--
-- Purpose:
--   Confirm the CONTROL tables exist and are ready for Airflow integration.
--
-- This script does NOT insert or modify operational audit data.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ACT_DB;
USE SCHEMA CONTROL;


-- ============================================================================
-- 1. CONFIRM TABLES
-- ============================================================================

SHOW TABLES IN SCHEMA ACT_DB.CONTROL;


-- ============================================================================
-- 2. CURRENT ROW COUNTS
--
-- Expected immediately after creation:
--   all counts can be 0 because Airflow has not been wired to write audit
--   records yet.
-- ============================================================================

SELECT
    'PIPELINE_RUN_AUDIT' AS TABLE_NAME,
    COUNT(*) AS ROW_COUNT
FROM ACT_DB.CONTROL.PIPELINE_RUN_AUDIT

UNION ALL

SELECT
    'ENTITY_LOAD_AUDIT',
    COUNT(*)
FROM ACT_DB.CONTROL.ENTITY_LOAD_AUDIT

UNION ALL

SELECT
    'REPROCESS_AUDIT',
    COUNT(*)
FROM ACT_DB.CONTROL.REPROCESS_AUDIT

ORDER BY TABLE_NAME;


-- ============================================================================
-- 3. VERIFY IMPORTANT COLUMNS
-- ============================================================================

SELECT
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    COLUMN_DEFAULT
FROM ACT_DB.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'CONTROL'
  AND TABLE_NAME IN
  (
      'PIPELINE_RUN_AUDIT',
      'ENTITY_LOAD_AUDIT',
      'REPROCESS_AUDIT'
  )
ORDER BY
    TABLE_NAME,
    ORDINAL_POSITION;
