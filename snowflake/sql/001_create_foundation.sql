-- ============================================================================
-- ACT Data Platform - Snowflake Foundation
-- File: snowflake/sql/001_create_foundation.sql
--
-- Purpose:
--   Create the core ACT Snowflake database and schemas.
--
-- Design:
--   RAW
--     - RAW landing tables
--     - External stage object
--     - Named file formats used to load RAW data
--
--   CONTROL
--     - Load audit / control metadata
--     - Snowflake-side operational metadata added later
--
-- Note:
--   There is intentionally NO separate STAGE schema.
--   Snowflake separates Tables, Stages, and File Formats as different object
--   types inside the RAW schema.
-- ============================================================================

USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS ACT_DB
    COMMENT = 'ACT clinical data platform';

CREATE SCHEMA IF NOT EXISTS ACT_DB.RAW
    COMMENT = 'ACT raw landing tables plus RAW ingestion stage/file-format objects';

CREATE SCHEMA IF NOT EXISTS ACT_DB.CONTROL
    COMMENT = 'ACT operational control and load-audit objects';

SHOW SCHEMAS IN DATABASE ACT_DB;