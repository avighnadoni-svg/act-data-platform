-- ============================================================================
-- ACT Data Platform - Snowflake Foundation
-- File: snowflake/sql/001_create_foundation.sql
--
-- Purpose:
--   Create the core Snowflake database and schemas used by ACT.
--
-- Architecture:
--
--   ACT_DB
--      |
--      +-- RAW
--      |    |
--      |    +-- internal RAW stage
--      |    +-- landing tables
--      |    +-- RAW history tables
--      |    +-- RAW current tables
--      |
--      +-- CONTROL
--           |
--           +-- pipeline audit
--           +-- entity audit
--           +-- watermark
--           +-- reprocess audit
--
-- Storage is intentionally independent from AWS.
-- ============================================================================

USE ROLE ACCOUNTADMIN;


CREATE DATABASE IF NOT EXISTS ACT_DB
    COMMENT = 'ACT clinical data platform';


CREATE SCHEMA IF NOT EXISTS ACT_DB.RAW
    COMMENT = 'ACT RAW ingestion and current/history data layer';


CREATE SCHEMA IF NOT EXISTS ACT_DB.CONTROL
    COMMENT = 'ACT operational control, watermark and audit layer';


SHOW SCHEMAS
IN DATABASE ACT_DB;