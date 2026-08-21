-- ============================================================================
-- ACT Data Platform
-- Snowflake Internal RAW Stage
--
-- Local development:
--
-- Local filesystem
--       |
--       | PUT
--       v
-- @ACT_DB.RAW.ACT_RAW_STAGE
--       |
--       | COPY INTO
--       v
-- RAW processing
--
-- No AWS / S3 dependency is required.
-- ============================================================================


-- ============================================================================
-- DATABASE CONTEXT
-- ============================================================================

USE DATABASE ACT_DB;

USE SCHEMA RAW;


-- ============================================================================
-- COMMON CSV FILE FORMAT
-- ============================================================================

CREATE FILE FORMAT IF NOT EXISTS ACT_RAW_CSV_FF

    TYPE = CSV

    FIELD_DELIMITER = ','

    SKIP_HEADER = 1

    FIELD_OPTIONALLY_ENCLOSED_BY = '"'

    EMPTY_FIELD_AS_NULL = TRUE

    NULL_IF = (
        '',
        'NULL',
        'null'
    )

    TRIM_SPACE = FALSE

    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE

    ENCODING = 'UTF8'

    COMMENT =
        'Common CSV file format for normalized ACT RAW files';


-- ============================================================================
-- INTERNAL RAW STAGE
-- ============================================================================

CREATE STAGE IF NOT EXISTS ACT_RAW_STAGE

    FILE_FORMAT = ACT_RAW_CSV_FF

    DIRECTORY = (
        ENABLE = TRUE
    )

    COMMENT =
        'Internal RAW stage for ACT local and storage-neutral ingestion';


-- ============================================================================
-- VALIDATION
-- ============================================================================

DESC FILE FORMAT ACT_RAW_CSV_FF;

DESC STAGE ACT_RAW_STAGE;

LIST @ACT_RAW_STAGE;