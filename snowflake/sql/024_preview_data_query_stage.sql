-- ============================================================================
-- ACT Data Platform - Preview DATA_QUERY normalized S3 file
-- File: snowflake/sql/024_preview_data_query_stage.sql
--
-- Purpose:
--   Inspect the exact normalized CSV column order before creating the
--   LND / HISTORY / CURRENT Snowflake tables.
--
-- Why:
--   DATA_QUERY is sourced as XML, then parsed/normalized to CSV by the
--   Airflow/Python ingestion layer. We confirm the final CSV order first
--   instead of guessing positional COPY mappings.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ACT_DB;
USE SCHEMA RAW;


-- ============================================================================
-- 1. SHOW AVAILABLE DATA_QUERY FILES
-- ============================================================================

LIST @ACT_DB.RAW.ACT_RAW_S3_STAGE
    PATTERN = '.*data_query/.*/data_query[.]csv';


-- ============================================================================
-- 2. PREVIEW RAW POSITIONAL VALUES
-- ============================================================================

SELECT
    METADATA$FILENAME          AS SOURCE_FILE_NAME,
    METADATA$FILE_ROW_NUMBER   AS SOURCE_FILE_ROW_NUMBER,

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

FROM @ACT_DB.RAW.ACT_RAW_S3_STAGE
(
    PATTERN => '.*data_query/.*/data_query[.]csv'
) t

ORDER BY
    SOURCE_FILE_NAME,
    SOURCE_FILE_ROW_NUMBER

LIMIT 20;
