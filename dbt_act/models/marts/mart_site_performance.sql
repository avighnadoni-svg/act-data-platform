{{
    config(
        materialized='table',

        contract={
            "enforced": true
        }
    )
}}

with site as (

    select *
    from {{ ref('dim_site') }}

),

study as (

    select *
    from {{ ref('dim_study') }}

),

-- ============================================================
-- SUBJECT METRICS
-- ============================================================

subject_summary as (

    select

        site_key,

        count(*) as subject_count,

        count_if(
            enrollment_date is not null
        ) as enrolled_subject_count

    from {{ ref('dim_subject') }}

    group by site_key

),

-- ============================================================
-- ADVERSE EVENT METRICS
-- ============================================================

ae_summary as (

    select

        site_key,

        sum(adverse_event_count)
            as adverse_event_count,

        count_if(
            serious = 'Y'
        ) as serious_ae_count,

        count_if(
            requires_safety_review = true
        ) as safety_review_ae_count

    from {{ ref('fct_adverse_event') }}

    group by site_key

),

-- ============================================================
-- LAB METRICS
-- ============================================================

lab_summary as (

    select

        site_key,

        sum(lab_result_count)
            as lab_result_count,

        count_if(
            abnormal = true
        ) as abnormal_lab_count

    from {{ ref('fct_lab_result') }}

    group by site_key

),

-- ============================================================
-- PROTOCOL DEVIATION METRICS
-- ============================================================

protocol_deviation_summary as (

    select

        site_key,

        sum(protocol_deviation_count)
            as protocol_deviation_count,

        count_if(
            upper(severity) = 'MAJOR'
        ) as major_deviation_count,

        count_if(
            upper(severity) = 'CRITICAL'
        ) as critical_deviation_count

    from {{ ref('fct_protocol_deviation') }}

    where site_key is not null

    group by site_key

),

-- ============================================================
-- DATA QUERY METRICS
-- ============================================================

data_query_summary as (

    select

        site_key,

        sum(data_query_count)
            as data_query_count,

        sum(open_query_count)
            as open_query_count,

        sum(resolved_query_count)
            as resolved_query_count,

        avg(
            case
                when resolution_days is not null
                then resolution_days
            end
        ) as avg_resolution_days

    from {{ ref('fct_data_query') }}

    group by site_key

),

-- ============================================================
-- FINAL SITE PERFORMANCE MART
-- ============================================================

final as (

    select

        -- ========================================================
        -- KEYS
        -- ========================================================

        cast(site.site_key as varchar)
            as site_key,

        cast(site.study_key as varchar)
            as study_key,

        cast(site.study_id as varchar)
            as study_id,

        cast(site.site_id as varchar)
            as site_id,


        -- ========================================================
        -- STUDY CONTEXT
        -- ========================================================

        cast(study.study_name as varchar)
            as study_name,

        cast(study.phase as varchar)
            as phase,


        -- ========================================================
        -- SITE CONTEXT
        -- ========================================================

        cast(site.country as varchar)
            as country,

        cast(site.investigator as varchar)
            as investigator,

        cast(site.target_enrollment as number(38,0))
            as target_enrollment,


        -- ========================================================
        -- ENROLLMENT METRICS
        -- ========================================================

        cast(
            coalesce(
                subjects.subject_count,
                0
            )
            as number(38,0)
        ) as subject_count,

        cast(
            coalesce(
                subjects.enrolled_subject_count,
                0
            )
            as number(38,0)
        ) as enrolled_subject_count,

        cast(
            site.target_enrollment
            -
            coalesce(
                subjects.subject_count,
                0
            )
            as number(38,0)
        ) as enrollment_gap,

        cast(
            round(
                (
                    coalesce(
                        subjects.subject_count,
                        0
                    ) * 100.0
                )
                /
                nullif(
                    site.target_enrollment,
                    0
                ),
                2
            )
            as number(10,2)
        ) as enrollment_pct,


        -- ========================================================
        -- ADVERSE EVENTS
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


        -- ========================================================
        -- LAB RESULTS
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
        -- PROTOCOL DEVIATIONS
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
        -- DATA QUERIES
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
            dq.avg_resolution_days
            as number(10,2)
        ) as avg_query_resolution_days,


        -- ========================================================
        -- DBT AUDIT
        -- ========================================================

        cast(
            current_timestamp()
            as timestamp_ltz
        ) as dbt_loaded_at

    from site

    left join study
        on site.study_key = study.study_key

    left join subject_summary as subjects
        on site.site_key = subjects.site_key

    left join ae_summary as ae
        on site.site_key = ae.site_key

    left join lab_summary as lab
        on site.site_key = lab.site_key

    left join protocol_deviation_summary as pd
        on site.site_key = pd.site_key

    left join data_query_summary as dq
        on site.site_key = dq.site_key

)

select *
from final