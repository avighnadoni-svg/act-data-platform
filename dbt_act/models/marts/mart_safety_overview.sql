{{
    config(
        materialized='table',

        contract={
            "enforced": true
        }
    )
}}

with subject as (

    select *
    from {{ ref('dim_subject') }}

),

site as (

    select *
    from {{ ref('dim_site') }}

),

study as (

    select *
    from {{ ref('dim_study') }}

),

-- ============================================================
-- ADVERSE EVENT SAFETY METRICS
-- ============================================================

ae_summary as (

    select

        subject_key,

        sum(adverse_event_count)
            as adverse_event_count,

        count_if(
            upper(serious) = 'Y'
        ) as serious_ae_count,

        count_if(
            upper(severity) = 'SEVERE'
        ) as severe_ae_count,

        count_if(
            requires_safety_review = true
        ) as safety_review_ae_count,

        avg(
            reporting_delay_days
        ) as avg_ae_reporting_delay_days,

        max(
            reporting_delay_days
        ) as max_ae_reporting_delay_days

    from {{ ref('fct_adverse_event') }}

    group by subject_key

),

-- ============================================================
-- LAB SAFETY METRICS
-- ============================================================

lab_summary as (

    select

        subject_key,

        sum(lab_result_count)
            as lab_result_count,

        count_if(
            abnormal = true
        ) as abnormal_lab_count,

        max(
            abs(range_variance)
        ) as max_absolute_range_variance

    from {{ ref('fct_lab_result') }}

    group by subject_key

),

-- ============================================================
-- FINAL
-- ============================================================

final as (

    select

        -- ========================================================
        -- KEYS
        -- ========================================================

        cast(subject.subject_key as varchar)
            as subject_key,

        cast(subject.site_key as varchar)
            as site_key,

        cast(subject.study_key as varchar)
            as study_key,

        cast(subject.study_id as varchar)
            as study_id,

        cast(subject.site_id as varchar)
            as site_id,

        cast(subject.subject_id as varchar)
            as subject_id,


        -- ========================================================
        -- STUDY / SITE CONTEXT
        -- ========================================================

        cast(study.study_name as varchar)
            as study_name,

        cast(study.phase as varchar)
            as phase,

        cast(site.country as varchar)
            as country,

        cast(site.investigator as varchar)
            as investigator,


        -- ========================================================
        -- SUBJECT CONTEXT
        -- ========================================================

        cast(subject.gender as varchar)
            as gender,

        cast(subject.age as number(38,0))
            as age,

        cast(subject.status as varchar)
            as subject_status,

        cast(subject.enrollment_date as date)
            as enrollment_date,


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
                ae.severe_ae_count,
                0
            )
            as number(38,0)
        ) as severe_ae_count,

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

        cast(
            ae.max_ae_reporting_delay_days
            as number(38,0)
        ) as max_ae_reporting_delay_days,


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

        cast(
            lab.max_absolute_range_variance
            as float
        ) as max_absolute_range_variance,


        -- ========================================================
        -- OPERATIONAL SAFETY FLAGS
        -- ========================================================

        cast(
            case
                when coalesce(
                    ae.serious_ae_count,
                    0
                ) > 0
                then true

                else false
            end
            as boolean
        ) as has_serious_ae,

        cast(
            case
                when coalesce(
                    ae.severe_ae_count,
                    0
                ) > 0
                then true

                else false
            end
            as boolean
        ) as has_severe_ae,

        cast(
            case
                when coalesce(
                    lab.abnormal_lab_count,
                    0
                ) > 0
                then true

                else false
            end
            as boolean
        ) as has_abnormal_lab,

        cast(
            case

                when coalesce(
                    ae.serious_ae_count,
                    0
                ) > 0
                then true

                when coalesce(
                    ae.safety_review_ae_count,
                    0
                ) > 0
                then true

                when coalesce(
                    lab.abnormal_lab_count,
                    0
                ) > 0
                then true

                else false

            end
            as boolean
        ) as requires_safety_attention,


        -- ========================================================
        -- DBT AUDIT
        -- ========================================================

        cast(
            current_timestamp()
            as timestamp_ltz
        ) as dbt_loaded_at

    from subject

    left join study
        on subject.study_key = study.study_key

    left join site
        on subject.site_key = site.site_key

    left join ae_summary as ae
        on subject.subject_key = ae.subject_key

    left join lab_summary as lab
        on subject.subject_key = lab.subject_key

)

select *
from final