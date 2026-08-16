-- ============================================================================
-- ACT Data Platform - Validate Snowflake S3 Storage Integration
-- File: snowflake/sql/004_validate_storage_integration.sql
--
-- Purpose:
--   Validate that Snowflake can assume the AWS IAM role and LIST objects under
--   the ACT RAW S3 prefix.
--
-- Notes:
--   - The IAM role is intentionally read-only.
--   - Therefore this validates only the LIST action, not WRITE/DELETE.
-- ============================================================================

USE ROLE ACCOUNTADMIN;

SELECT SYSTEM$VALIDATE_STORAGE_INTEGRATION(
    'ACT_S3_INT',
    's3://act-clinical-data-dev/act/raw/',
    'act_storage_validation.txt',
    'list'
) AS VALIDATION_RESULT;