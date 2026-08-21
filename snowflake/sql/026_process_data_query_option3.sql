-- ============================================================================
-- ACT Data Platform - Process DATA_QUERY Option 3
-- File: snowflake/sql/026_process_data_query_option3.sql
--
-- Confirmed normalized CSV order:
--
--   1  query_id
--   2  study_id
--   3  subject_id
--   4  site_id
--   5  opened_date
--   6  resolved_date
--   7  status
--   8  updated_at
--   9  source_system
--   10 source_entity
--   11 dag_run_id
--   12 ingested_at
--
-- Replay protection:
--   HISTORY uniqueness =
--       STUDY_ID + QUERY_ID + UPDATED_AT + RECORD_HASH
--
-- Current-state rule:
--   Most recently received version wins.
--
-- This supports:
--   - normal incremental loads
--   - replayed source files
--   - historical reprocessing
--   - corrections where business data changes but UPDATED_AT remains unchanged
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ACT_DB;
USE SCHEMA RAW;

ALTER SESSION SET ERROR_ON_NONDETERMINISTIC_MERGE = TRUE;


-- ============================================================================
-- 1. COPY NEW DATA_QUERY FILES INTO LANDING
-- ============================================================================

COPY INTO ACT_DB.RAW.LND_DATA_QUERY
(
    QUERY_ID,
    STUDY_ID,
    SUBJECT_ID,
    SITE_ID,
    OPENED_DATE,
    RESOLVED_DATE,
    STATUS,
    UPDATED_AT,

    SOURCE_SYSTEM,
    SOURCE_ENTITY,
    DAG_RUN_ID,
    INGESTED_AT,

    SOURCE_FILE_NAME,
    SOURCE_FILE_ROW_NUMBER,
    SOURCE_FILE_CONTENT_KEY,
    SOURCE_FILE_LAST_MODIFIED,
    SNOWFLAKE_LOAD_TS
)
FROM
(
    SELECT
        t.$1::VARCHAR,
        t.$2::VARCHAR,
        t.$3::VARCHAR,
        t.$4::VARCHAR,
        TRY_TO_DATE(t.$5::VARCHAR),
        TRY_TO_DATE(t.$6::VARCHAR),
        t.$7::VARCHAR,
        TRY_TO_TIMESTAMP_TZ(t.$8::VARCHAR),

        t.$9::VARCHAR,
        t.$10::VARCHAR,
        t.$11::VARCHAR,
        TRY_TO_TIMESTAMP_TZ(t.$12::VARCHAR),

        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        METADATA$FILE_CONTENT_KEY,
        METADATA$FILE_LAST_MODIFIED,
        METADATA$START_SCAN_TIME

    FROM @ACT_DB.RAW.ACT_RAW_STAGE
    (
        PATTERN => '.*data_query/.*/data_query[.]csv'
    ) t
)
ON_ERROR = 'ABORT_STATEMENT'
FORCE = FALSE;


-- ============================================================================
-- 2. CALCULATE BUSINESS-CONTENT HASH
-- ============================================================================

UPDATE ACT_DB.RAW.LND_DATA_QUERY
SET RECORD_HASH =
    SHA2(
        CONCAT_WS(
            '||',
            COALESCE(SUBJECT_ID, '<NULL>'),
            COALESCE(SITE_ID, '<NULL>'),
            COALESCE(TO_VARCHAR(OPENED_DATE, 'YYYY-MM-DD'), '<NULL>'),
            COALESCE(TO_VARCHAR(RESOLVED_DATE, 'YYYY-MM-DD'), '<NULL>'),
            COALESCE(STATUS, '<NULL>')
        ),
        256
    )
WHERE PROCESSED_FLAG = FALSE
  AND RECORD_HASH IS NULL;


-- ============================================================================
-- 3. VALIDATE UNPROCESSED LANDING
-- ============================================================================

SELECT
    COUNT(*) AS UNPROCESSED_ROWS,
    COUNT_IF(STUDY_ID IS NULL) AS NULL_STUDY_ID_ROWS,
    COUNT_IF(QUERY_ID IS NULL) AS NULL_QUERY_ID_ROWS,
    COUNT_IF(SUBJECT_ID IS NULL) AS NULL_SUBJECT_ID_ROWS,
    COUNT_IF(SITE_ID IS NULL) AS NULL_SITE_ID_ROWS,
    COUNT_IF(OPENED_DATE IS NULL) AS NULL_OPENED_DATE_ROWS,
    COUNT_IF(STATUS IS NULL) AS NULL_STATUS_ROWS,
    COUNT_IF(RECORD_HASH IS NULL) AS NULL_HASH_ROWS
FROM ACT_DB.RAW.LND_DATA_QUERY
WHERE PROCESSED_FLAG = FALSE;


-- ============================================================================
-- 4. ADD UNIQUE VERSIONS TO HISTORY
-- ============================================================================

MERGE INTO ACT_DB.RAW.RAW_DATA_QUERY_HISTORY AS T
USING
(
    SELECT
        QUERY_ID,
        STUDY_ID,
        SUBJECT_ID,
        SITE_ID,
        OPENED_DATE,
        RESOLVED_DATE,
        STATUS,
        UPDATED_AT,
        RECORD_HASH,

        SOURCE_SYSTEM,
        SOURCE_ENTITY,
        DAG_RUN_ID,
        INGESTED_AT,

        SOURCE_FILE_NAME,
        SOURCE_FILE_ROW_NUMBER,
        SOURCE_FILE_CONTENT_KEY,
        SOURCE_FILE_LAST_MODIFIED,
        SNOWFLAKE_LOAD_TS

    FROM ACT_DB.RAW.LND_DATA_QUERY
    WHERE PROCESSED_FLAG = FALSE

    QUALIFY ROW_NUMBER() OVER
    (
        PARTITION BY
            STUDY_ID,
            QUERY_ID,
            UPDATED_AT,
            RECORD_HASH
        ORDER BY
            INGESTED_AT DESC NULLS LAST,
            SNOWFLAKE_LOAD_TS DESC NULLS LAST,
            SOURCE_FILE_LAST_MODIFIED DESC NULLS LAST,
            SOURCE_FILE_NAME DESC,
            SOURCE_FILE_ROW_NUMBER DESC
    ) = 1
) AS S

ON  T.STUDY_ID = S.STUDY_ID
AND T.QUERY_ID = S.QUERY_ID
AND T.UPDATED_AT = S.UPDATED_AT
AND T.RECORD_HASH = S.RECORD_HASH

WHEN NOT MATCHED THEN
INSERT
(
    QUERY_ID,
    STUDY_ID,
    SUBJECT_ID,
    SITE_ID,
    OPENED_DATE,
    RESOLVED_DATE,
    STATUS,
    UPDATED_AT,
    RECORD_HASH,

    SOURCE_SYSTEM,
    SOURCE_ENTITY,
    DAG_RUN_ID,
    INGESTED_AT,

    SOURCE_FILE_NAME,
    SOURCE_FILE_ROW_NUMBER,
    SOURCE_FILE_CONTENT_KEY,
    SOURCE_FILE_LAST_MODIFIED,
    SNOWFLAKE_LOAD_TS
)
VALUES
(
    S.QUERY_ID,
    S.STUDY_ID,
    S.SUBJECT_ID,
    S.SITE_ID,
    S.OPENED_DATE,
    S.RESOLVED_DATE,
    S.STATUS,
    S.UPDATED_AT,
    S.RECORD_HASH,

    S.SOURCE_SYSTEM,
    S.SOURCE_ENTITY,
    S.DAG_RUN_ID,
    S.INGESTED_AT,

    S.SOURCE_FILE_NAME,
    S.SOURCE_FILE_ROW_NUMBER,
    S.SOURCE_FILE_CONTENT_KEY,
    S.SOURCE_FILE_LAST_MODIFIED,
    S.SNOWFLAKE_LOAD_TS
);


-- ============================================================================
-- 5. MERGE LATEST-RECEIVED VERSION INTO CURRENT
-- ============================================================================

MERGE INTO ACT_DB.RAW.RAW_DATA_QUERY_CURRENT AS T
USING
(
    SELECT
        QUERY_ID,
        STUDY_ID,
        SUBJECT_ID,
        SITE_ID,
        OPENED_DATE,
        RESOLVED_DATE,
        STATUS,
        UPDATED_AT,
        RECORD_HASH,

        SOURCE_SYSTEM,
        SOURCE_ENTITY,
        DAG_RUN_ID,
        INGESTED_AT,

        SOURCE_FILE_NAME,
        SOURCE_FILE_ROW_NUMBER,
        SOURCE_FILE_CONTENT_KEY,
        SOURCE_FILE_LAST_MODIFIED,
        SNOWFLAKE_LOAD_TS

    FROM ACT_DB.RAW.LND_DATA_QUERY
    WHERE PROCESSED_FLAG = FALSE

    QUALIFY ROW_NUMBER() OVER
    (
        PARTITION BY
            STUDY_ID,
            QUERY_ID
        ORDER BY
            INGESTED_AT DESC NULLS LAST,
            SNOWFLAKE_LOAD_TS DESC NULLS LAST,
            SOURCE_FILE_LAST_MODIFIED DESC NULLS LAST,
            UPDATED_AT DESC NULLS LAST,
            SOURCE_FILE_NAME DESC,
            SOURCE_FILE_ROW_NUMBER DESC
    ) = 1
) AS S

ON  T.STUDY_ID = S.STUDY_ID
AND T.QUERY_ID = S.QUERY_ID

WHEN MATCHED
AND
(
       T.RECORD_HASH <> S.RECORD_HASH
    OR T.UPDATED_AT <> S.UPDATED_AT
)
AND
    COALESCE(S.INGESTED_AT, S.SNOWFLAKE_LOAD_TS)
    >=
    COALESCE(T.INGESTED_AT, T.SNOWFLAKE_LOAD_TS)

THEN UPDATE SET
    T.SUBJECT_ID = S.SUBJECT_ID,
    T.SITE_ID = S.SITE_ID,
    T.OPENED_DATE = S.OPENED_DATE,
    T.RESOLVED_DATE = S.RESOLVED_DATE,
    T.STATUS = S.STATUS,
    T.UPDATED_AT = S.UPDATED_AT,
    T.RECORD_HASH = S.RECORD_HASH,

    T.SOURCE_SYSTEM = S.SOURCE_SYSTEM,
    T.SOURCE_ENTITY = S.SOURCE_ENTITY,
    T.DAG_RUN_ID = S.DAG_RUN_ID,
    T.INGESTED_AT = S.INGESTED_AT,

    T.SOURCE_FILE_NAME = S.SOURCE_FILE_NAME,
    T.SOURCE_FILE_ROW_NUMBER = S.SOURCE_FILE_ROW_NUMBER,
    T.SOURCE_FILE_CONTENT_KEY = S.SOURCE_FILE_CONTENT_KEY,
    T.SOURCE_FILE_LAST_MODIFIED = S.SOURCE_FILE_LAST_MODIFIED,
    T.SNOWFLAKE_LOAD_TS = S.SNOWFLAKE_LOAD_TS,

    T.CURRENT_ROW_UPDATED_AT = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
INSERT
(
    QUERY_ID,
    STUDY_ID,
    SUBJECT_ID,
    SITE_ID,
    OPENED_DATE,
    RESOLVED_DATE,
    STATUS,
    UPDATED_AT,
    RECORD_HASH,

    SOURCE_SYSTEM,
    SOURCE_ENTITY,
    DAG_RUN_ID,
    INGESTED_AT,

    SOURCE_FILE_NAME,
    SOURCE_FILE_ROW_NUMBER,
    SOURCE_FILE_CONTENT_KEY,
    SOURCE_FILE_LAST_MODIFIED,
    SNOWFLAKE_LOAD_TS,

    CURRENT_ROW_UPDATED_AT
)
VALUES
(
    S.QUERY_ID,
    S.STUDY_ID,
    S.SUBJECT_ID,
    S.SITE_ID,
    S.OPENED_DATE,
    S.RESOLVED_DATE,
    S.STATUS,
    S.UPDATED_AT,
    S.RECORD_HASH,

    S.SOURCE_SYSTEM,
    S.SOURCE_ENTITY,
    S.DAG_RUN_ID,
    S.INGESTED_AT,

    S.SOURCE_FILE_NAME,
    S.SOURCE_FILE_ROW_NUMBER,
    S.SOURCE_FILE_CONTENT_KEY,
    S.SOURCE_FILE_LAST_MODIFIED,
    S.SNOWFLAKE_LOAD_TS,

    CURRENT_TIMESTAMP()
);


-- ============================================================================
-- 6. MARK LANDING ROWS PROCESSED
-- ============================================================================

UPDATE ACT_DB.RAW.LND_DATA_QUERY
SET
    PROCESSED_FLAG = TRUE,
    PROCESSED_AT = CURRENT_TIMESTAMP()
WHERE PROCESSED_FLAG = FALSE;


-- ============================================================================
-- 7. VALIDATION
-- ============================================================================

SELECT
    PROCESSED_FLAG,
    COUNT(*) AS LANDING_ROWS
FROM ACT_DB.RAW.LND_DATA_QUERY
GROUP BY PROCESSED_FLAG
ORDER BY PROCESSED_FLAG;


SELECT
    STUDY_ID,
    COUNT(*) AS HISTORY_ROWS,
    COUNT(DISTINCT QUERY_ID) AS DISTINCT_QUERIES
FROM ACT_DB.RAW.RAW_DATA_QUERY_HISTORY
GROUP BY STUDY_ID
ORDER BY STUDY_ID;


SELECT
    STUDY_ID,
    COUNT(*) AS CURRENT_ROWS,
    COUNT(DISTINCT QUERY_ID) AS DISTINCT_QUERIES
FROM ACT_DB.RAW.RAW_DATA_QUERY_CURRENT
GROUP BY STUDY_ID
ORDER BY STUDY_ID;


-- Must return zero rows.
SELECT
    STUDY_ID,
    QUERY_ID,
    COUNT(*) AS CURRENT_ROW_COUNT
FROM ACT_DB.RAW.RAW_DATA_QUERY_CURRENT
GROUP BY
    STUDY_ID,
    QUERY_ID
HAVING COUNT(*) > 1;


SELECT
    STUDY_ID,
    QUERY_ID,
    SUBJECT_ID,
    SITE_ID,
    OPENED_DATE,
    RESOLVED_DATE,
    STATUS,
    UPDATED_AT,
    RECORD_HASH,
    INGESTED_AT
FROM ACT_DB.RAW.RAW_DATA_QUERY_CURRENT
ORDER BY
    STUDY_ID,
    QUERY_ID;


SELECT
    FILE_NAME,
    STATUS,
    ROW_COUNT,
    ROW_PARSED,
    ERROR_COUNT,
    FIRST_ERROR_MESSAGE,
    LAST_LOAD_TIME
FROM ACT_DB.INFORMATION_SCHEMA.LOAD_HISTORY
WHERE
    SCHEMA_NAME = 'RAW'
    AND TABLE_NAME = 'LND_DATA_QUERY'
ORDER BY LAST_LOAD_TIME DESC;
