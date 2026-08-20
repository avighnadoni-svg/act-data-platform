{{ config(materialized='view') }}

with adverse_event as (

    select *
    from {{ ref('stg_adverse_event') }}

),

subject_context as (

    select *
    from {{ ref('int_subject_context') }}

),

adverse_event_context as (

    select

        -- ========================================================
        -- ADVERSE EVENT
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
        -- SUBJECT CONTEXT
        -- ========================================================

        subject.site_id,
        subject.gender,
        subject.age,
        subject.subject_status,
        subject.enrollment_date,

        -- ========================================================
        -- SITE CONTEXT
        -- ========================================================

        subject.country,
        subject.investigator,
        subject.target_enrollment,

        -- ========================================================
        -- STUDY CONTEXT
        -- ========================================================

        subject.study_name,
        subject.phase,
        subject.target_subjects,

        -- ========================================================
        -- TRACEABILITY
        -- ========================================================

        ae.source_updated_at as ae_source_updated_at,
        subject.subject_source_updated_at,
        subject.site_source_updated_at,
        subject.study_source_updated_at,

        ae.record_hash,
        ae.dag_run_id,
        ae.ingested_at

    from adverse_event as ae

    left join subject_context as subject
        on ae.study_id = subject.study_id
       and ae.subject_id = subject.subject_id

)

select *
from adverse_event_context