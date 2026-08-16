-- ============================================================================
-- ACT Data Platform - Verify Snowflake Foundation
-- File: snowflake/sql/003_verify_foundation.sql
-- Purpose: Verify the database/schemas and display the storage integration
--          when it exists.
-- ============================================================================

USE ROLE ACCOUNTADMIN;

SHOW DATABASES LIKE 'ACT_DB';
SHOW SCHEMAS IN DATABASE ACT_DB;

-- Run this statement after 002_create_storage_integration.sql has succeeded.
-- DESC INTEGRATION ACT_S3_INT;
