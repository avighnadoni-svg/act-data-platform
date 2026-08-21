{{
    config(
        materialized='table',

        contract={
            "enforced": true
        }
    )
}}

with study as (

    select *
    from {{ ref('dim_study') }}

),

-- ============================================================
-- SITE METRICS
-- ============================================================

site_summary as (

    select

        study_key,

        count(*) as site_count,

        sum(
            coalesce(target_enrollment, 0)
        ) as total_site_target_enrollment

    from {{ ref('dim_site') }}

    group by study_key

),

-- ============================================================
-- SUBJECT METRICS
-- ============================================================

subject_summary as (

    select

        study_key,

        count(*) as subject_count,

        count_if(
            enrollment_date is not null
        ) as enrolled_subject_count

    from {{ ref('dim_subject') }}

    group by study_key

),

-- ============================================================
-- VISIT METRICS
-- ============================================================

visit_summary as (

    select

        study_key,

        count(*) as visit_count,

        count_if(
            actual_date is not null
        ) as completed_visit_count,

        count_if(
            actual_date is null
        ) as pending_visit_count

    from {{ ref('dim_visit') }}

    group by study_key

),

-- ============================================================
-- ADVERSE EVENT METRICS
-- ============================================================

ae_summary as (

    select

        study_key,

        sum(adverse_event_count)
            as adverse_event_count,

        count_if(
            serious = 'Y'
        ) as serious_ae_count,

        count_if(
            requires_safety_review = true
        ) as safety_review_ae_count,

        avg(
            reporting_delay_days
        ) as avg_ae_reporting_delay_days

    from {{ ref('fct_adverse_event') }}

    group by study_key

),

-- ============================================================
-- LAB METRICS
-- ============================================================

lab_summary as (

    select

        study_key,

        sum(lab_result_count)
            as lab_result_count,

        count_if(
            abnormal = true
        ) as abnormal_lab_count

    from {{ ref('fct_lab_result') }}

    group by study_key

),

-- ============================================================
-- PROTOCOL DEVIATION METRICS
-- ============================================================

protocol_deviation_summary as (

    select

        study_key,

        sum(protocol_deviation_count)
            as protocol_deviation_count,

        count_if(
            upper(severity) = 'MAJOR'
        ) as major_deviation_count,

        count_if(
            upper(severity) = 'CRITICAL'
        ) as critical_deviation_count

    from {{ ref('fct_protocol_deviation') }}

    group by study_key

),

-- ============================================================
-- DATA QUERY METRICS
-- ============================================================

data_query_summary as (

    select

        study_key,

        sum(data_query_count)
            as data_query_count,

        sum(open_query_count)
            as open_query_count,

        sum(resolved_query_count)
            as resolved_query_count,

        avg(
            resolution_days
        ) as avg_query_resolution_days

    from {{ ref('fct_data_query') }}

    group by study_key

),

-- ============================================================
-- FINAL STUDY OVERVIEW
-- ============================================================

final as (

    select

        -- ========================================================
        -- STUDY
        -- ========================================================

        cast(study.study_key as varchar)
            as study_key,

        cast(study.study_id as varchar)
            as study_id,

        cast(study.study_name as varchar)
            as study_name,

        cast(study.phase as varchar)
            as phase,

        cast(study.target_subjects as number(38,0))
            as target_subjects,


        -- ========================================================
        -- SITE METRICS
        -- ========================================================

        cast(
            coalesce(site.site_count, 0)
            as number(38,0)
        ) as site_count,

        cast(
            coalesce(
                site.total_site_target_enrollment,
                0
            )
            as number(38,0)
        ) as total_site_target_enrollment,


        -- ========================================================
        -- SUBJECT / ENROLLMENT METRICS
        -- ========================================================

        cast(
            coalesce(subject.subject_count, 0)
            as number(38,0)
        ) as subject_count,

        cast(
            coalesce(
                subject.enrolled_subject_count,
                0
            )
            as number(38,0)
        ) as enrolled_subject_count,

        cast(
            study.target_subjects
            -
            coalesce(subject.subject_count, 0)
            as number(38,0)
        ) as enrollment_gap,

        cast(
            round(
                (
                    coalesce(
                        subject.subject_count,
                        0
                    ) * 100.0
                )
                /
                nullif(
                    study.target_subjects,
                    0
                ),
                2
            )
            as number(10,2)
        ) as enrollment_pct,


        -- ========================================================
        -- VISIT METRICS
        -- ========================================================

        cast(
            coalesce(visit.visit_count, 0)
            as number(38,0)
        ) as visit_count,

        cast(
            coalesce(
                visit.completed_visit_count,
                0
            )
            as number(38,0)
        ) as completed_visit_count,

        cast(
            coalesce(
                visit.pending_visit_count,
                0
            )
            as number(38,0)
        ) as pending_visit_count,


        -- ========================================================
        -- ADVERSE EVENT METRICS
        -- ========================================================

        cast(
            coalesce(
                ae.adverse_event_count,
                0
            )
            as number(38,0)
        ) as adverse_event_count,

        cast(
            coalesce(
                ae.serious_ae_count,
                0
            )
            as number(38,0)
        ) as serious_ae_count,

        cast(
            coalesce(
                ae.safety_review_ae_count,
                0
            )
            as number(38,0)
        ) as safety_review_ae_count,

        cast(
            ae.avg_ae_reporting_delay_days
            as number(10,2)
        ) as avg_ae_reporting_delay_days,


        -- ========================================================
        -- LAB METRICS
        -- ========================================================

        cast(
            coalesce(
                lab.lab_result_count,
                0
            )
            as number(38,0)
        ) as lab_result_count,

        cast(
            coalesce(
                lab.abnormal_lab_count,
                0
            )
            as number(38,0)
        ) as abnormal_lab_count,


        -- ========================================================
        -- PROTOCOL DEVIATION METRICS
        -- ========================================================

        cast(
            coalesce(
                pd.protocol_deviation_count,
                0
            )
            as number(38,0)
        ) as protocol_deviation_count,

        cast(
            coalesce(
                pd.major_deviation_count,
                0
            )
            as number(38,0)
        ) as major_deviation_count,

        cast(
            coalesce(
                pd.critical_deviation_count,
                0
            )
            as number(38,0)
        ) as critical_deviation_count,


        -- ========================================================
        -- DATA QUERY METRICS
        -- ========================================================

        cast(
            coalesce(
                dq.data_query_count,
                0
            )
            as number(38,0)
        ) as data_query_count,

        cast(
            coalesce(
                dq.open_query_count,
                0
            )
            as number(38,0)
        ) as open_query_count,

        cast(
            coalesce(
                dq.resolved_query_count,
                0
            )
            as number(38,0)
        ) as resolved_query_count,

        cast(
            dq.avg_query_resolution_days
            as number(10,2)
        ) as avg_query_resolution_days,


        -- ========================================================
        -- DBT AUDIT
        -- ========================================================

        cast(
            current_timestamp()
            as timestamp_ltz
        ) as dbt_loaded_at

    from study

    left join site_summary as site
        on study.study_key = site.study_key

    left join subject_summary as subject
        on study.study_key = subject.study_key

    left join visit_summary as visit
        on study.study_key = visit.study_key

    left join ae_summary as ae
        on study.study_key = ae.study_key

    left join lab_summary as lab
        on study.study_key = lab.study_key

    left join protocol_deviation_summary as pd
        on study.study_key = pd.study_key

    left join data_query_summary as dq
        on study.study_key = dq.study_key

)

select *
from final