-- ============================================================================
-- ACT Data Platform - Migrate CONTROL Audit Storage Columns
-- File: snowflake/sql/033_migrate_control_storage_columns.sql
--
-- Purpose:
--
--   Remove AWS/S3-specific terminology from the operational CONTROL layer.
--
-- Before:
--
--   S3_ROW_COUNT
--   S3_URI
--
-- After:
--
--   STORAGE_ROW_COUNT
--   STORAGE_URI
--
-- This preserves existing audit data because the columns are renamed,
-- not dropped and recreated.
--
-- Run this migration once.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ACT_DB;
USE SCHEMA CONTROL;


-- ============================================================================
-- 1. ENTITY_LOAD_AUDIT
-- ============================================================================

ALTER TABLE ACT_DB.CONTROL.ENTITY_LOAD_AUDIT
RENAME COLUMN S3_ROW_COUNT TO STORAGE_ROW_COUNT;


ALTER TABLE ACT_DB.CONTROL.ENTITY_LOAD_AUDIT
RENAME COLUMN S3_URI TO STORAGE_URI;


-- ============================================================================
-- 2. REPROCESS_AUDIT
-- ============================================================================

ALTER TABLE ACT_DB.CONTROL.REPROCESS_AUDIT
RENAME COLUMN S3_URI TO STORAGE_URI;


-- ============================================================================
-- 3. VERIFY ENTITY_LOAD_AUDIT
-- ============================================================================

SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE
FROM ACT_DB.INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_SCHEMA = 'CONTROL'
    AND TABLE_NAME = 'ENTITY_LOAD_AUDIT'
ORDER BY
    ORDINAL_POSITION;


-- ============================================================================
-- 4. VERIFY REPROCESS_AUDIT
-- ============================================================================

SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE
FROM ACT_DB.INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_SCHEMA = 'CONTROL'
    AND TABLE_NAME = 'REPROCESS_AUDIT'
ORDER BY
    ORDINAL_POSITION;