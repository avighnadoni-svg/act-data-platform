-- ============================================================================
-- ACT Data Platform - Verify Snowflake Foundation
-- File: snowflake/sql/003_verify_foundation.sql
--
-- Purpose:
--   Verify ACT database, schemas, RAW internal stage and file format.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE ACT_DB;


SHOW DATABASES
LIKE 'ACT_DB';


SHOW SCHEMAS
IN DATABASE ACT_DB;


SHOW STAGES
IN SCHEMA ACT_DB.RAW;


SHOW FILE FORMATS
IN SCHEMA ACT_DB.RAW;


SHOW TABLES
IN SCHEMA ACT_DB.RAW;


SHOW TABLES
IN SCHEMA ACT_DB.CONTROL;