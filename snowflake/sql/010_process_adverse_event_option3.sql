-- ============================================================================
-- ACT Data Platform - Process Adverse Event: S3 -> LND -> HISTORY + CURRENT
-- File: snowflake/sql/010_process_adverse_event_option3.sql
--
-- Flow:
--
--   S3 adverse_event CSV files
--          |
--          v
--   COPY INTO LND_ADVERSE_EVENT
--          |
--          +--> HISTORY MERGE
--          |      preserves each unique source version
--          |
--          +--> CURRENT MERGE
--                 keeps one latest-received row per STUDY_ID + AE_ID
--
-- Important behavior:
--
--   1. COPY uses FORCE = FALSE.
--      Snowflake skips files already loaded into LND_ADVERSE_EVENT.
--
--   2. LND rows are retained and marked PROCESSED_FLAG = TRUE.
--      We do NOT truncate LND, so COPY load history remains useful and the
--      landing table also provides operational traceability.
--
--   3. HISTORY suppresses exact replay duplicates:
--         STUDY_ID + AE_ID + UPDATED_AT + RECORD_HASH
--
--   4. CURRENT uses the most recently RECEIVED version.
--      This intentionally supports client corrections where business data
--      changes but the source UPDATED_AT value is not changed.
--
--   5. RECORD_HASH contains business/content fields only. File/run/audit
--      metadata is excluded so a replay of unchanged content does not look
--      like a business change.
--
-- Re-running this script is designed to be safe.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ACT_DB;
USE SCHEMA RAW;

ALTER SESSION SET ERROR_ON_NONDETERMINISTIC_MERGE = TRUE;


-- ============================================================================
-- 1. COPY NEW S3 FILES INTO LANDING
-- ============================================================================
--
-- Because LND_ADVERSE_EVENT is a NEW COPY target, the first execution of this
-- script will load all adverse_event files currently visible in S3 once.
--
-- That is expected.
--
-- 009 already migrated the legacy RAW data into HISTORY/CURRENT, so the
-- HISTORY merge below will suppress exact versions that already exist.
--
-- On future executions, FORCE = FALSE prevents normally reloading the same
-- physical S3 files into LND_ADVERSE_EVENT.
-- ============================================================================

COPY INTO ACT_DB.RAW.LND_ADVERSE_EVENT
(
    AE_ID,
    STUDY_ID,
    SUBJECT_ID,
    EVENT_TERM,
    SEVERITY,
    SERIOUS,
    EVENT_DATE,
    REPORTED_DATE,
    PROCESSING_PRIORITY,
    REQUIRES_SAFETY_REVIEW,
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
        t.$5::VARCHAR,
        t.$6::VARCHAR,
        TRY_TO_DATE(t.$7::VARCHAR),
        TRY_TO_DATE(t.$8::VARCHAR),
        t.$9::VARCHAR,
        TRY_TO_BOOLEAN(t.$10::VARCHAR),
        TRY_TO_TIMESTAMP_TZ(t.$11::VARCHAR),

        t.$12::VARCHAR,
        t.$13::VARCHAR,
        t.$14::VARCHAR,
        TRY_TO_TIMESTAMP_TZ(t.$15::VARCHAR),

        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        METADATA$FILE_CONTENT_KEY,
        METADATA$FILE_LAST_MODIFIED,
        METADATA$START_SCAN_TIME

    FROM @ACT_DB.RAW.ACT_RAW_S3_STAGE
    (
        PATTERN => '.*adverse_event/.*/adverse_event[.]csv'
    ) t
)
ON_ERROR = 'ABORT_STATEMENT'
FORCE = FALSE;


-- ============================================================================
-- 2. CALCULATE RECORD HASH FOR UNPROCESSED LANDING ROWS
-- ============================================================================
--
-- Business/content fields included:
--   SUBJECT_ID
--   EVENT_TERM
--   SEVERITY
--   SERIOUS
--   EVENT_DATE
--   REPORTED_DATE
--   PROCESSING_PRIORITY
--   REQUIRES_SAFETY_REVIEW
--
-- STUDY_ID + AE_ID are the business key, so they are not needed inside the
-- content hash itself.
--
-- UPDATED_AT is deliberately excluded because a business change must still be
-- detected when the client does not change UPDATED_AT.
-- ============================================================================

UPDATE ACT_DB.RAW.LND_ADVERSE_EVENT
SET RECORD_HASH =
    SHA2(
        CONCAT_WS(
            '||',
            COALESCE(SUBJECT_ID, '<NULL>'),
            COALESCE(EVENT_TERM, '<NULL>'),
            COALESCE(SEVERITY, '<NULL>'),
            COALESCE(SERIOUS, '<NULL>'),
            COALESCE(TO_VARCHAR(EVENT_DATE, 'YYYY-MM-DD'), '<NULL>'),
            COALESCE(TO_VARCHAR(REPORTED_DATE, 'YYYY-MM-DD'), '<NULL>'),
            COALESCE(PROCESSING_PRIORITY, '<NULL>'),
            COALESCE(TO_VARCHAR(REQUIRES_SAFETY_REVIEW), '<NULL>')
        ),
        256
    )
WHERE PROCESSED_FLAG = FALSE
  AND RECORD_HASH IS NULL;


-- ============================================================================
-- 3. VALIDATE THE UNPROCESSED BATCH
-- ============================================================================

SELECT
    COUNT(*) AS UNPROCESSED_ROWS,
    COUNT_IF(RECORD_HASH IS NULL) AS NULL_HASH_ROWS,
    COUNT_IF(STUDY_ID IS NULL) AS NULL_STUDY_ID_ROWS,
    COUNT_IF(AE_ID IS NULL) AS NULL_AE_ID_ROWS
FROM ACT_DB.RAW.LND_ADVERSE_EVENT
WHERE PROCESSED_FLAG = FALSE;


-- ============================================================================
-- 4. ADD UNIQUE VERSIONS TO HISTORY
-- ============================================================================
--
-- The source is deduplicated first so one target version can match at most
-- one source row during the MERGE.
--
-- Exact replay definition:
--
--   same STUDY_ID
--   same AE_ID
--   same UPDATED_AT
--   same RECORD_HASH
--
-- Therefore:
--
--   Same content + same timestamp replay  -> ignored
--   Changed content + same timestamp      -> new HISTORY version
--   Same content + changed timestamp      -> new HISTORY version
--   Changed content + changed timestamp   -> new HISTORY version
-- ============================================================================

MERGE INTO ACT_DB.RAW.RAW_ADVERSE_EVENT_HISTORY AS T
USING
(
    SELECT
        AE_ID,
        STUDY_ID,
        SUBJECT_ID,
        EVENT_TERM,
        SEVERITY,
        SERIOUS,
        EVENT_DATE,
        REPORTED_DATE,
        PROCESSING_PRIORITY,
        REQUIRES_SAFETY_REVIEW,
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

    FROM ACT_DB.RAW.LND_ADVERSE_EVENT
    WHERE PROCESSED_FLAG = FALSE

    QUALIFY ROW_NUMBER() OVER
    (
        PARTITION BY
            STUDY_ID,
            AE_ID,
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
AND T.AE_ID = S.AE_ID
AND T.UPDATED_AT = S.UPDATED_AT
AND T.RECORD_HASH = S.RECORD_HASH

WHEN NOT MATCHED THEN
INSERT
(
    AE_ID,
    STUDY_ID,
    SUBJECT_ID,
    EVENT_TERM,
    SEVERITY,
    SERIOUS,
    EVENT_DATE,
    REPORTED_DATE,
    PROCESSING_PRIORITY,
    REQUIRES_SAFETY_REVIEW,
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
    S.AE_ID,
    S.STUDY_ID,
    S.SUBJECT_ID,
    S.EVENT_TERM,
    S.SEVERITY,
    S.SERIOUS,
    S.EVENT_DATE,
    S.REPORTED_DATE,
    S.PROCESSING_PRIORITY,
    S.REQUIRES_SAFETY_REVIEW,
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
-- 5. MERGE THE LATEST-RECEIVED VERSION INTO CURRENT
-- ============================================================================
--
-- Important:
--   CURRENT is based on latest receipt into ACT, not solely MAX(UPDATED_AT).
--
-- This supports:
--
--   old version:
--     AE10201 | MILD   | UPDATED_AT Aug-07 | INGESTED_AT Aug-07
--
--   client correction received later:
--     AE10201 | SEVERE | UPDATED_AT Aug-07 | INGESTED_AT Aug-17
--
-- Result:
--     CURRENT = SEVERE
--
-- The source is reduced to exactly one row per STUDY_ID + AE_ID so MERGE is
-- deterministic.
-- ============================================================================

MERGE INTO ACT_DB.RAW.RAW_ADVERSE_EVENT_CURRENT AS T
USING
(
    SELECT
        AE_ID,
        STUDY_ID,
        SUBJECT_ID,
        EVENT_TERM,
        SEVERITY,
        SERIOUS,
        EVENT_DATE,
        REPORTED_DATE,
        PROCESSING_PRIORITY,
        REQUIRES_SAFETY_REVIEW,
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

    FROM ACT_DB.RAW.LND_ADVERSE_EVENT
    WHERE PROCESSED_FLAG = FALSE

    QUALIFY ROW_NUMBER() OVER
    (
        PARTITION BY STUDY_ID, AE_ID
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
AND T.AE_ID = S.AE_ID

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
    T.EVENT_TERM = S.EVENT_TERM,
    T.SEVERITY = S.SEVERITY,
    T.SERIOUS = S.SERIOUS,
    T.EVENT_DATE = S.EVENT_DATE,
    T.REPORTED_DATE = S.REPORTED_DATE,
    T.PROCESSING_PRIORITY = S.PROCESSING_PRIORITY,
    T.REQUIRES_SAFETY_REVIEW = S.REQUIRES_SAFETY_REVIEW,
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
    AE_ID,
    STUDY_ID,
    SUBJECT_ID,
    EVENT_TERM,
    SEVERITY,
    SERIOUS,
    EVENT_DATE,
    REPORTED_DATE,
    PROCESSING_PRIORITY,
    REQUIRES_SAFETY_REVIEW,
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
    S.AE_ID,
    S.STUDY_ID,
    S.SUBJECT_ID,
    S.EVENT_TERM,
    S.SEVERITY,
    S.SERIOUS,
    S.EVENT_DATE,
    S.REPORTED_DATE,
    S.PROCESSING_PRIORITY,
    S.REQUIRES_SAFETY_REVIEW,
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
--
-- This occurs only after both HISTORY and CURRENT DML statements above have
-- completed successfully.
-- ============================================================================

UPDATE ACT_DB.RAW.LND_ADVERSE_EVENT
SET
    PROCESSED_FLAG = TRUE,
    PROCESSED_AT = CURRENT_TIMESTAMP()
WHERE PROCESSED_FLAG = FALSE;


-- ============================================================================
-- 7. VALIDATION / OBSERVABILITY
-- ============================================================================

-- Landing processing status.
SELECT
    PROCESSED_FLAG,
    COUNT(*) AS ROW_COUNT
FROM ACT_DB.RAW.LND_ADVERSE_EVENT
GROUP BY PROCESSED_FLAG
ORDER BY PROCESSED_FLAG;


-- History versions by study.
SELECT
    STUDY_ID,
    COUNT(*) AS HISTORY_ROWS,
    COUNT(DISTINCT AE_ID) AS DISTINCT_AES
FROM ACT_DB.RAW.RAW_ADVERSE_EVENT_HISTORY
GROUP BY STUDY_ID
ORDER BY STUDY_ID;


-- Current must be exactly one row per business key.
SELECT
    STUDY_ID,
    COUNT(*) AS CURRENT_ROWS,
    COUNT(DISTINCT AE_ID) AS DISTINCT_AES
FROM ACT_DB.RAW.RAW_ADVERSE_EVENT_CURRENT
GROUP BY STUDY_ID
ORDER BY STUDY_ID;


-- This must return zero rows.
SELECT
    STUDY_ID,
    AE_ID,
    COUNT(*) AS CURRENT_ROW_COUNT
FROM ACT_DB.RAW.RAW_ADVERSE_EVENT_CURRENT
GROUP BY
    STUDY_ID,
    AE_ID
HAVING COUNT(*) > 1;


-- Show the latest/current state.
SELECT
    STUDY_ID,
    AE_ID,
    EVENT_TERM,
    SEVERITY,
    UPDATED_AT,
    RECORD_HASH,
    DAG_RUN_ID,
    INGESTED_AT,
    CURRENT_ROW_UPDATED_AT
FROM ACT_DB.RAW.RAW_ADVERSE_EVENT_CURRENT
ORDER BY
    STUDY_ID,
    AE_ID;


-- Show business keys with more than one historical version.
SELECT
    STUDY_ID,
    AE_ID,
    COUNT(*) AS VERSION_COUNT
FROM ACT_DB.RAW.RAW_ADVERSE_EVENT_HISTORY
GROUP BY
    STUDY_ID,
    AE_ID
HAVING COUNT(*) > 1
ORDER BY
    STUDY_ID,
    AE_ID;


-- COPY load history for the landing table.
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
    AND TABLE_NAME = 'LND_ADVERSE_EVENT'
ORDER BY LAST_LOAD_TIME DESC;
