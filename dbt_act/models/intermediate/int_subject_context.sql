{{ config(materialized='view') }}

with subject as (

    select *
    from {{ ref('stg_subject') }}

),

site as (

    select *
    from {{ ref('stg_site') }}

),

study as (

    select *
    from {{ ref('stg_study') }}

),

subject_context as (

    select

        -- ========================================================
        -- STUDY CONTEXT
        -- ========================================================

        subject.study_id,
        study.study_name,
        study.phase,
        study.target_subjects,

        -- ========================================================
        -- SITE CONTEXT
        -- ========================================================

        subject.site_id,
        site.country,
        site.investigator,
        site.target_enrollment,

        -- ========================================================
        -- SUBJECT
        -- ========================================================

        subject.subject_id,
        subject.gender,
        subject.age,
        subject.status as subject_status,
        subject.enrollment_date,

        -- ========================================================
        -- SOURCE TIMESTAMPS
        -- ========================================================

        subject.source_updated_at as subject_source_updated_at,
        site.source_updated_at as site_source_updated_at,
        study.source_updated_at as study_source_updated_at

    from subject

    left join site
        on subject.study_id = site.study_id
       and subject.site_id = site.site_id

    left join study
        on subject.study_id = study.study_id

)

select *
from subject_context