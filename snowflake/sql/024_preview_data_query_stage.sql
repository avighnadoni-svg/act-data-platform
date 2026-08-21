-- ============================================================================
-- ACT Data Platform - Preview DATA_QUERY Internal Stage
-- File: snowflake/sql/024_preview_data_query_stage.sql
--
-- Purpose:
--   Inspect normalized data_query CSV files in the Snowflake internal
--   RAW stage.
--
-- Source format may be XML, but the Python ingestion layer normalizes
-- records to CSV before Snowflake loading.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ACT_DB;
USE SCHEMA RAW;


-- ============================================================================
-- FILES
-- ============================================================================

LIST @ACT_DB.RAW.ACT_RAW_STAGE
PATTERN =
    '.*data_query/.*/data_query[.]csv';


-- ============================================================================
-- POSITIONAL PREVIEW
-- ============================================================================

SELECT
    METADATA$FILENAME
        AS SOURCE_FILE_NAME,

    METADATA$FILE_ROW_NUMBER
        AS SOURCE_FILE_ROW_NUMBER,

    t.$1::VARCHAR  AS COL_01,
    t.$2::VARCHAR  AS COL_02,
    t.$3::VARCHAR  AS COL_03,
    t.$4::VARCHAR  AS COL_04,
    t.$5::VARCHAR  AS COL_05,
    t.$6::VARCHAR  AS COL_06,
    t.$7::VARCHAR  AS COL_07,
    t.$8::VARCHAR  AS COL_08,
    t.$9::VARCHAR  AS COL_09,
    t.$10::VARCHAR AS COL_10,
    t.$11::VARCHAR AS COL_11,
    t.$12::VARCHAR AS COL_12,
    t.$13::VARCHAR AS COL_13,
    t.$14::VARCHAR AS COL_14,
    t.$15::VARCHAR AS COL_15,
    t.$16::VARCHAR AS COL_16

FROM
    @ACT_DB.RAW.ACT_RAW_STAGE
(
    PATTERN =>
        '.*data_query/.*/data_query[.]csv'
) t

ORDER BY
    SOURCE_FILE_NAME,
    SOURCE_FILE_ROW_NUMBER

LIMIT 20;