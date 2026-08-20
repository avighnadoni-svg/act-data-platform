{{ config(materialized='view') }}

with adverse_event as (

    select *
    from {{ ref('int_adverse_event_context') }}

),

-- ============================================================
-- VISITS
-- ============================================================

visit_summary as (

    select
        study_id,
        subject_id,

        count(*) as visit_count,

        max(actual_date)
            as latest_actual_visit_date,

        max(planned_date)
            as latest_planned_visit_date,

        max(source_updated_at)
            as visit_context_updated_at

    from {{ ref('stg_visit') }}

    group by
        study_id,
        subject_id

),

-- ============================================================
-- LAB RESULTS
-- ============================================================

lab_summary as (

    select
        study_id,
        subject_id,

        count(*) as lab_result_count,

        count_if(abnormal = true)
            as abnormal_lab_count,

        max(source_updated_at)
            as lab_context_updated_at

    from {{ ref('stg_lab_result') }}

    group by
        study_id,
        subject_id

),

-- ============================================================
-- PROTOCOL DEVIATIONS
-- ============================================================

protocol_deviation_summary as (

    select
        study_id,
        subject_id,

        count(*) as protocol_deviation_count,

        count_if(severity = 'MAJOR')
            as major_deviation_count,

        count_if(severity = 'CRITICAL')
            as critical_deviation_count,

        max(source_updated_at)
            as protocol_deviation_context_updated_at

    from {{ ref('stg_protocol_deviation') }}

    where subject_id is not null

    group by
        study_id,
        subject_id

),

-- ============================================================
-- DATA QUERIES
-- ============================================================

data_query_summary as (

    select
        study_id,
        subject_id,

        count(*) as data_query_count,

        count_if(resolved_date is null)
            as unresolved_query_count,

        max(source_updated_at)
            as data_query_context_updated_at

    from {{ ref('stg_data_query') }}

    group by
        study_id,
        subject_id

),

-- ============================================================
-- ENRICH AE
-- ============================================================

enriched as (

    select

        -- ========================================================
        -- AE
        -- ========================================================

        ae.study_id,
        ae.ae_id,
        ae.subject_id,

        ae.event_term,
        ae.severity,
        ae.serious,

        ae.event_date,
        ae.reported_date,

        ae.processing_priority,
        ae.requires_safety_review,

        -- ========================================================
        -- SUBJECT
        -- ========================================================

        ae.site_id,
        ae.gender,
        ae.age,
        ae.subject_status,
        ae.enrollment_date,

        -- ========================================================
        -- SITE
        -- ========================================================

        ae.country,
        ae.investigator,
        ae.target_enrollment,

        -- ========================================================
        -- STUDY
        -- ========================================================

        ae.study_name,
        ae.phase,
        ae.target_subjects,

        -- ========================================================
        -- VISITS
        -- ========================================================

        coalesce(
            visit.visit_count,
            0
        ) as visit_count,

        visit.latest_actual_visit_date,
        visit.latest_planned_visit_date,

        -- ========================================================
        -- LABS
        -- ========================================================

        coalesce(
            lab.lab_result_count,
            0
        ) as lab_result_count,

        coalesce(
            lab.abnormal_lab_count,
            0
        ) as abnormal_lab_count,

        -- ========================================================
        -- PROTOCOL DEVIATIONS
        -- ========================================================

        coalesce(
            pd.protocol_deviation_count,
            0
        ) as protocol_deviation_count,

        coalesce(
            pd.major_deviation_count,
            0
        ) as major_deviation_count,

        coalesce(
            pd.critical_deviation_count,
            0
        ) as critical_deviation_count,

        -- ========================================================
        -- DATA QUERIES
        -- ========================================================

        coalesce(
            dq.data_query_count,
            0
        ) as data_query_count,

        coalesce(
            dq.unresolved_query_count,
            0
        ) as unresolved_query_count,

        -- ========================================================
        -- INDIVIDUAL CHANGE TIMESTAMPS
        -- ========================================================

        ae.ae_source_updated_at,
        ae.subject_source_updated_at,
        ae.site_source_updated_at,
        ae.study_source_updated_at,

        visit.visit_context_updated_at,
        lab.lab_context_updated_at,

        pd.protocol_deviation_context_updated_at,

        dq.data_query_context_updated_at,

        -- ========================================================
        -- TRACEABILITY
        -- ========================================================

        ae.record_hash,
        ae.dag_run_id,
        ae.ingested_at

    from adverse_event as ae

    left join visit_summary as visit
        on ae.study_id = visit.study_id
       and ae.subject_id = visit.subject_id

    left join lab_summary as lab
        on ae.study_id = lab.study_id
       and ae.subject_id = lab.subject_id

    left join protocol_deviation_summary as pd
        on ae.study_id = pd.study_id
       and ae.subject_id = pd.subject_id

    left join data_query_summary as dq
        on ae.study_id = dq.study_id
       and ae.subject_id = dq.subject_id

),

-- ============================================================
-- CALCULATE THE CHANGE WATERMARK
--
-- Any change to:
-- AE
-- Subject
-- Site
-- Study
-- Visit
-- Lab
-- Protocol Deviation
-- Data Query
--
-- should make this AE eligible for incremental reprocessing.
-- ============================================================

final as (

    select
        *,

        greatest(

            coalesce(
                ae_source_updated_at,
                cast(
                    '1900-01-01 00:00:00 +00:00'
                    as timestamp_tz
                )
            ),

            coalesce(
                subject_source_updated_at,
                cast(
                    '1900-01-01 00:00:00 +00:00'
                    as timestamp_tz
                )
            ),

            coalesce(
                site_source_updated_at,
                cast(
                    '1900-01-01 00:00:00 +00:00'
                    as timestamp_tz
                )
            ),

            coalesce(
                study_source_updated_at,
                cast(
                    '1900-01-01 00:00:00 +00:00'
                    as timestamp_tz
                )
            ),

            coalesce(
                visit_context_updated_at,
                cast(
                    '1900-01-01 00:00:00 +00:00'
                    as timestamp_tz
                )
            ),

            coalesce(
                lab_context_updated_at,
                cast(
                    '1900-01-01 00:00:00 +00:00'
                    as timestamp_tz
                )
            ),

            coalesce(
                protocol_deviation_context_updated_at,
                cast(
                    '1900-01-01 00:00:00 +00:00'
                    as timestamp_tz
                )
            ),

            coalesce(
                data_query_context_updated_at,
                cast(
                    '1900-01-01 00:00:00 +00:00'
                    as timestamp_tz
                )
            )

        ) as operational_context_updated_at

    from enriched

)

select *
from final