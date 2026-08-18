-- ============================================================================
-- ACT Data Platform - Preview PROTOCOL_DEVIATION normalized S3 file
-- File: snowflake/sql/021_preview_protocol_deviation_stage.sql
--
-- Purpose:
--   Inspect the exact normalized CSV column order before creating the
--   LND / HISTORY / CURRENT Snowflake tables.
--
-- Why:
--   Protocol Deviation is delivered as CSV from the source API, but before
--   defining Snowflake positional COPY mappings we want to confirm the actual
--   normalized output written by the current Airflow/Python normalizer.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ACT_DB;
USE SCHEMA RAW;


-- ============================================================================
-- 1. SHOW AVAILABLE PROTOCOL_DEVIATION FILES
-- ============================================================================

LIST @ACT_DB.RAW.ACT_RAW_S3_STAGE
    PATTERN = '.*protocol_deviation/.*/protocol_deviation[.]csv';


-- ============================================================================
-- 2. PREVIEW RAW POSITIONAL VALUES
--
-- We intentionally do NOT assign business-column names yet.
-- This avoids guessing the current normalizer output order.
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
    t.$15::VARCHAR AS COL_15

FROM @ACT_DB.RAW.ACT_RAW_S3_STAGE
(
    PATTERN => '.*protocol_deviation/.*/protocol_deviation[.]csv'
) t

ORDER BY
    SOURCE_FILE_NAME,
    SOURCE_FILE_ROW_NUMBER

LIMIT 20;
