-- ============================================================================
-- ACT Data Platform - Seed Snowflake WATERMARK from ENTITY_LOAD_AUDIT
-- File: snowflake/sql/031_seed_watermark_from_entity_audit.sql
--
-- Purpose:
--   Populate ACT_DB.CONTROL.WATERMARK using audit rows produced by the
--   ACT ingestion tasks that already ran successfully.
--
-- Why:
--   In this Airflow 3 lab, the standalone migration script could not see
--   the existing Task SDK Variables outside task execution. However,
--   ENTITY_LOAD_AUDIT already contains SOURCE_WATERMARK_TO for each
--   successfully audited study/entity extraction.
--
-- Single source of truth after cutover:
--   ACT_DB.CONTROL.WATERMARK
--
-- Safe rerun behavior:
--   - Inserts missing study/entity watermarks.
--   - Updates an existing watermark only when the audit watermark is
--     equal to or newer than the stored watermark.
--   - Never moves a watermark backwards.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ACT_DB;
USE SCHEMA CONTROL;


-- ============================================================================
-- 1. PRE-CHECK: SHOW THE LATEST USABLE AUDIT WATERMARK FOR EACH STUDY/ENTITY
-- ============================================================================

SELECT
    STUDY_ID,
    ENTITY_NAME,
    STATUS,
    SOURCE_WATERMARK_FROM,
    SOURCE_WATERMARK_TO,
    DAG_RUN_ID,
    ENDED_AT
FROM ACT_DB.CONTROL.ENTITY_LOAD_AUDIT
WHERE STATUS IN ('SUCCESS', 'NO_NEW_DATA')
  AND SOURCE_WATERMARK_TO IS NOT NULL

QUALIFY ROW_NUMBER() OVER
(
    PARTITION BY
        STUDY_ID,
        ENTITY_NAME
    ORDER BY
        ENDED_AT DESC NULLS LAST,
        UPDATED_AT DESC NULLS LAST,
        CREATED_AT DESC NULLS LAST
) = 1

ORDER BY
    STUDY_ID,
    ENTITY_NAME;


-- ============================================================================
-- 2. SEED / REFRESH WATERMARK TABLE
-- ============================================================================

MERGE INTO ACT_DB.CONTROL.WATERMARK AS T

USING
(
    SELECT
        STUDY_ID,
        ENTITY_NAME,
        SOURCE_WATERMARK_TO AS WATERMARK_VALUE,
        DAG_RUN_ID AS LAST_SUCCESSFUL_RUN_ID

    FROM ACT_DB.CONTROL.ENTITY_LOAD_AUDIT

    WHERE STATUS IN ('SUCCESS', 'NO_NEW_DATA')
      AND SOURCE_WATERMARK_TO IS NOT NULL

    QUALIFY ROW_NUMBER() OVER
    (
        PARTITION BY
            STUDY_ID,
            ENTITY_NAME
        ORDER BY
            ENDED_AT DESC NULLS LAST,
            UPDATED_AT DESC NULLS LAST,
            CREATED_AT DESC NULLS LAST
    ) = 1
) AS S

ON  T.STUDY_ID = S.STUDY_ID
AND T.ENTITY_NAME = S.ENTITY_NAME


WHEN MATCHED
AND S.WATERMARK_VALUE >= T.WATERMARK_VALUE
THEN UPDATE SET

    T.WATERMARK_VALUE =
        S.WATERMARK_VALUE,

    T.LAST_SUCCESSFUL_RUN_ID =
        S.LAST_SUCCESSFUL_RUN_ID,

    T.UPDATED_AT =
        CURRENT_TIMESTAMP()


WHEN NOT MATCHED
THEN INSERT
(
    STUDY_ID,
    ENTITY_NAME,
    WATERMARK_VALUE,
    LAST_SUCCESSFUL_RUN_ID,
    CREATED_AT,
    UPDATED_AT
)
VALUES
(
    S.STUDY_ID,
    S.ENTITY_NAME,
    S.WATERMARK_VALUE,
    S.LAST_SUCCESSFUL_RUN_ID,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
);


-- ============================================================================
-- 3. FINAL WATERMARK TABLE
-- ============================================================================

SELECT
    STUDY_ID,
    ENTITY_NAME,
    WATERMARK_VALUE,
    LAST_SUCCESSFUL_RUN_ID,
    CREATED_AT,
    UPDATED_AT
FROM ACT_DB.CONTROL.WATERMARK
ORDER BY
    STUDY_ID,
    ENTITY_NAME;


-- ============================================================================
-- 4. COUNT VALIDATION
--
-- Current lab expectation:
--   2 studies * 8 entities = 16 rows
-- ============================================================================

SELECT
    COUNT(*) AS WATERMARK_ROW_COUNT,
    COUNT(DISTINCT STUDY_ID) AS STUDY_COUNT,
    COUNT(DISTINCT ENTITY_NAME) AS ENTITY_COUNT
FROM ACT_DB.CONTROL.WATERMARK;


-- ============================================================================
-- 5. DUPLICATE BUSINESS-KEY CHECK
--
-- Must return ZERO rows.
-- ============================================================================

SELECT
    STUDY_ID,
    ENTITY_NAME,
    COUNT(*) AS ROW_COUNT
FROM ACT_DB.CONTROL.WATERMARK
GROUP BY
    STUDY_ID,
    ENTITY_NAME
HAVING COUNT(*) > 1;


-- ============================================================================
-- 6. MISSING STUDY/ENTITY WATERMARK CHECK
--
-- Must return ZERO rows for the current ACT lab.
-- ============================================================================

WITH EXPECTED_ENTITIES AS
(
    SELECT COLUMN1 AS ENTITY_NAME
    FROM VALUES
        ('study'),
        ('site'),
        ('subject'),
        ('visit'),
        ('adverse_event'),
        ('lab_result'),
        ('protocol_deviation'),
        ('data_query')
),

EXPECTED_KEYS AS
(
    SELECT
        S.STUDY_ID,
        E.ENTITY_NAME

    FROM
    (
        SELECT DISTINCT STUDY_ID
        FROM ACT_DB.RAW.RAW_STUDY_CURRENT
        WHERE STUDY_ID IS NOT NULL
    ) S

    CROSS JOIN EXPECTED_ENTITIES E
)

SELECT
    E.STUDY_ID,
    E.ENTITY_NAME

FROM EXPECTED_KEYS E

LEFT JOIN ACT_DB.CONTROL.WATERMARK W
    ON  W.STUDY_ID = E.STUDY_ID
    AND W.ENTITY_NAME = E.ENTITY_NAME

WHERE W.STUDY_ID IS NULL

ORDER BY
    E.STUDY_ID,
    E.ENTITY_NAME;
