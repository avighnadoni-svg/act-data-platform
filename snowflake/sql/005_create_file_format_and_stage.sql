-- ============================================================================
-- ACT Data Platform - Create RAW File Format and External S3 Stage
-- File: snowflake/sql/005_create_file_format_and_stage.sql
--
-- Purpose:
--   Keep the S3 external stage and CSV file format directly in ACT_DB.RAW.
--
-- Final structure:
--
--   ACT_DB.RAW
--     Tables
--       RAW_ADVERSE_EVENT
--       ...future RAW tables
--
--     Stages
--       ACT_RAW_S3_STAGE
--
--     File Formats
--       ACT_RAW_CSV_FF
--
-- This file is safe for BOTH:
--   1. a fresh environment; and
--   2. the current development environment where ACT_DB.STAGE previously
--      contained the stage and file format.
--
-- It creates the correct RAW objects first, validates them, and only then
-- removes the obsolete ACT_DB.STAGE schema.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE ACT_DB;
USE SCHEMA RAW;

-- ---------------------------------------------------------------------------
-- 1. Common CSV file format for normalized ACT RAW files
-- ---------------------------------------------------------------------------

CREATE FILE FORMAT IF NOT EXISTS ACT_RAW_CSV_FF
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    EMPTY_FIELD_AS_NULL = TRUE
    NULL_IF = ('', 'NULL', 'null')
    TRIM_SPACE = FALSE
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
    ENCODING = 'UTF8'
    COMMENT = 'Common CSV file format for ACT normalized RAW S3 files';

-- ---------------------------------------------------------------------------
-- 2. External stage pointing at the ACT RAW S3 landing prefix
-- ---------------------------------------------------------------------------

CREATE STAGE IF NOT EXISTS ACT_RAW_S3_STAGE
    URL = 's3://act-clinical-data-dev/act/raw/'
    STORAGE_INTEGRATION = ACT_S3_INT
    FILE_FORMAT = ACT_RAW_CSV_FF
    COMMENT = 'External S3 stage for ACT RAW multi-study ingestion';

-- ---------------------------------------------------------------------------
-- 3. Validate the new RAW objects BEFORE removing the old schema
-- ---------------------------------------------------------------------------

DESC FILE FORMAT ACT_RAW_CSV_FF;
DESC STAGE ACT_RAW_S3_STAGE;

LIST @ACT_RAW_S3_STAGE;

-- ---------------------------------------------------------------------------
-- 4. Remove the old schema that was used only for stage/file-format objects
-- ---------------------------------------------------------------------------
-- CASCADE removes the old stage/file-format objects inside ACT_DB.STAGE.
-- It does NOT remove S3 files because ACT_RAW_S3_STAGE is an external stage.
-- ---------------------------------------------------------------------------

DROP SCHEMA IF EXISTS ACT_DB.STAGE CASCADE;

-- ---------------------------------------------------------------------------
-- 5. Final verification
-- ---------------------------------------------------------------------------

SHOW STAGES IN SCHEMA ACT_DB.RAW;
SHOW FILE FORMATS IN SCHEMA ACT_DB.RAW;
SHOW SCHEMAS IN DATABASE ACT_DB;