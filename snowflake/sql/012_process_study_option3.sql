-- ============================================================================
-- ACT Data Platform - Process STUDY Option 3
-- File: snowflake/sql/012_process_study_option3.sql
--
-- Flow:
--
--   S3
--    |
--    v
--   COPY INTO LND_STUDY
--    |
--    +--> HISTORY MERGE
--    |
--    +--> CURRENT MERGE
--
-- Replay protection:
--   HISTORY uniqueness =
--       STUDY_ID + UPDATED_AT + RECORD_HASH
--
-- Current-state rule:
--   Most recently received version wins.
--
-- This supports a client correction where STUDY data changes but UPDATED_AT
-- does not change.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ACT_DB;
USE SCHEMA RAW;

ALTER SESSION SET ERROR_ON_NONDETERMINISTIC_MERGE = TRUE;


-- ============================================================================
-- 1. COPY NEW STUDY FILES INTO LANDING
-- ============================================================================

COPY INTO ACT_DB.RAW.LND_STUDY
(
    STUDY_ID,
    STUDY_NAME,
    PHASE,
    TARGET_SUBJECTS,
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
        TRY_TO_NUMBER(t.$4::VARCHAR),
        TRY_TO_TIMESTAMP_TZ(t.$5::VARCHAR),

        t.$6::VARCHAR,
        t.$7::VARCHAR,
        t.$8::VARCHAR,
        TRY_TO_TIMESTAMP_TZ(t.$9::VARCHAR),

        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        METADATA$FILE_CONTENT_KEY,
        METADATA$FILE_LAST_MODIFIED,
        METADATA$START_SCAN_TIME

    FROM @ACT_DB.RAW.ACT_RAW_S3_STAGE
    (
        PATTERN => '.*study/.*/study[.]csv'
    ) t
)
ON_ERROR = 'ABORT_STATEMENT'
FORCE = FALSE;


-- ============================================================================
-- 2. CALCULATE BUSINESS-CONTENT HASH
-- ============================================================================

UPDATE ACT_DB.RAW.LND_STUDY
SET RECORD_HASH =
    SHA2(
        CONCAT_WS(
            '||',
            COALESCE(STUDY_NAME, '<NULL>'),
            COALESCE(PHASE, '<NULL>'),
            COALESCE(TO_VARCHAR(TARGET_SUBJECTS), '<NULL>')
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
    COUNT_IF(RECORD_HASH IS NULL) AS NULL_HASH_ROWS
FROM ACT_DB.RAW.LND_STUDY
WHERE PROCESSED_FLAG = FALSE;


-- ============================================================================
-- 4. ADD UNIQUE VERSIONS TO HISTORY
-- ============================================================================

MERGE INTO ACT_DB.RAW.RAW_STUDY_HISTORY AS T
USING
(
    SELECT
        STUDY_ID,
        STUDY_NAME,
        PHASE,
        TARGET_SUBJECTS,
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

    FROM ACT_DB.RAW.LND_STUDY
    WHERE PROCESSED_FLAG = FALSE

    QUALIFY ROW_NUMBER() OVER
    (
        PARTITION BY
            STUDY_ID,
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
AND T.UPDATED_AT = S.UPDATED_AT
AND T.RECORD_HASH = S.RECORD_HASH

WHEN NOT MATCHED THEN
INSERT
(
    STUDY_ID,
    STUDY_NAME,
    PHASE,
    TARGET_SUBJECTS,
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
    S.STUDY_ID,
    S.STUDY_NAME,
    S.PHASE,
    S.TARGET_SUBJECTS,
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

MERGE INTO ACT_DB.RAW.RAW_STUDY_CURRENT AS T
USING
(
    SELECT
        STUDY_ID,
        STUDY_NAME,
        PHASE,
        TARGET_SUBJECTS,
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

    FROM ACT_DB.RAW.LND_STUDY
    WHERE PROCESSED_FLAG = FALSE

    QUALIFY ROW_NUMBER() OVER
    (
        PARTITION BY STUDY_ID
        ORDER BY
            INGESTED_AT DESC NULLS LAST,
            SNOWFLAKE_LOAD_TS DESC NULLS LAST,
            SOURCE_FILE_LAST_MODIFIED DESC NULLS LAST,
            UPDATED_AT DESC NULLS LAST,
            SOURCE_FILE_NAME DESC,
            SOURCE_FILE_ROW_NUMBER DESC
    ) = 1
) AS S

ON T.STUDY_ID = S.STUDY_ID

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
    T.STUDY_NAME = S.STUDY_NAME,
    T.PHASE = S.PHASE,
    T.TARGET_SUBJECTS = S.TARGET_SUBJECTS,
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
    STUDY_ID,
    STUDY_NAME,
    PHASE,
    TARGET_SUBJECTS,
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
    S.STUDY_ID,
    S.STUDY_NAME,
    S.PHASE,
    S.TARGET_SUBJECTS,
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
-- 6. MARK LANDING PROCESSED
-- ============================================================================

UPDATE ACT_DB.RAW.LND_STUDY
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
FROM ACT_DB.RAW.LND_STUDY
GROUP BY PROCESSED_FLAG
ORDER BY PROCESSED_FLAG;


SELECT
    COUNT(*) AS HISTORY_ROWS,
    COUNT(DISTINCT STUDY_ID) AS DISTINCT_STUDIES
FROM ACT_DB.RAW.RAW_STUDY_HISTORY;


SELECT
    COUNT(*) AS CURRENT_ROWS,
    COUNT(DISTINCT STUDY_ID) AS DISTINCT_STUDIES
FROM ACT_DB.RAW.RAW_STUDY_CURRENT;


-- Must return zero rows.
SELECT
    STUDY_ID,
    COUNT(*) AS CURRENT_ROW_COUNT
FROM ACT_DB.RAW.RAW_STUDY_CURRENT
GROUP BY STUDY_ID
HAVING COUNT(*) > 1;


SELECT
    STUDY_ID,
    STUDY_NAME,
    PHASE,
    TARGET_SUBJECTS,
    UPDATED_AT,
    RECORD_HASH,
    INGESTED_AT
FROM ACT_DB.RAW.RAW_STUDY_CURRENT
ORDER BY STUDY_ID;


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
    AND TABLE_NAME = 'LND_STUDY'
ORDER BY LAST_LOAD_TIME DESC;
