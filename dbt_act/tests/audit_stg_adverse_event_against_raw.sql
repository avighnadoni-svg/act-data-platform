-- ============================================================
-- AUDIT
--
-- RAW_ADVERSE_EVENT_CURRENT
--              VS
-- STG_ADVERSE_EVENT
--
-- Purpose:
-- Prove that staging has not changed the business data.
--
-- Technical columns such as AE_KEY are intentionally excluded.
-- SOURCE_UPDATED_AT is renamed back to UPDATED_AT so both
-- queries have the same business structure.
-- ============================================================


{% set raw_query %}

    select
        study_id,
        ae_id,
        subject_id,

        event_term,
        severity,
        serious,

        event_date,
        reported_date,

        processing_priority,
        requires_safety_review,

        updated_at

    from {{ source('act_raw', 'adverse_event') }}

{% endset %}


{% set staging_query %}

    select
        study_id,
        ae_id,
        subject_id,

        event_term,
        severity,
        serious,

        event_date,
        reported_date,

        processing_priority,
        requires_safety_review,

        source_updated_at as updated_at

    from {{ ref('stg_adverse_event') }}

{% endset %}


with audit_results as (

    {{
        audit_helper.compare_and_classify_query_results(

            a_query = raw_query,

            b_query = staging_query,

            primary_key_columns = [
                'study_id',
                'ae_id'
            ],

            columns = [
                'study_id',
                'ae_id',
                'subject_id',
                'event_term',
                'severity',
                'serious',
                'event_date',
                'reported_date',
                'processing_priority',
                'requires_safety_review',
                'updated_at'
            ],

            sample_limit = 20

        )
    }}

)

-- ============================================================
-- dbt singular tests PASS when they return zero rows.
--
-- Therefore:
--
-- IDENTICAL → filtered out → PASS
-- MODIFIED  → returned     → FAIL
-- ADDED     → returned     → FAIL
-- REMOVED   → returned     → FAIL
-- ============================================================

select *
from audit_results

where dbt_audit_row_status <> 'identical'