{{
    config(
        materialized='table',

        contract={
            "enforced": true
        }
    )
}}

with lab_result as (

    select *
    from {{ ref('stg_lab_result') }}

),

subject_dimension as (

    select

        subject_key,
        site_key,
        study_key,
        study_id,
        site_id,
        subject_id

    from {{ ref('dim_subject') }}

),

final as (

    select

        -- ========================================================
        -- FACT KEY
        -- ========================================================

        cast(lab.lab_result_key as varchar)
            as lab_result_key,


        -- ========================================================
        -- DIMENSION KEYS
        -- ========================================================

        cast(lab.study_key as varchar)
            as study_key,

        cast(subject.site_key as varchar)
            as site_key,

        cast(lab.subject_key as varchar)
            as subject_key,


        -- ========================================================
        -- BUSINESS KEYS
        -- ========================================================

        cast(lab.study_id as varchar)
            as study_id,

        cast(subject.site_id as varchar)
            as site_id,

        cast(lab.subject_id as varchar)
            as subject_id,

        cast(lab.lab_id as varchar)
            as lab_id,


        -- ========================================================
        -- LAB ATTRIBUTES
        -- ========================================================

        cast(lab.test_name as varchar)
            as test_name,

        cast(lab.interpretation as varchar)
            as interpretation,

        cast(lab.abnormal as boolean)
            as abnormal,


        -- ========================================================
        -- LAB MEASURES
        -- ========================================================

        cast(lab.result_value as float)
            as result_value,

        cast(lab.normal_low as float)
            as normal_low,

        cast(lab.normal_high as float)
            as normal_high,

        cast(
            case

                when lab.result_value is null
                    then null

                when lab.normal_low is not null
                     and lab.result_value < lab.normal_low
                    then lab.result_value - lab.normal_low

                when lab.normal_high is not null
                     and lab.result_value > lab.normal_high
                    then lab.result_value - lab.normal_high

                else 0

            end
            as float
        ) as range_variance,

        cast(1 as number(38,0))
            as lab_result_count,


        -- ========================================================
        -- SOURCE TRACEABILITY
        -- ========================================================

        cast(lab.source_updated_at as timestamp_tz)
            as source_updated_at,

        cast(lab.record_hash as varchar)
            as record_hash,

        cast(lab.source_system as varchar)
            as source_system,

        cast(lab.source_entity as varchar)
            as source_entity,

        cast(lab.dag_run_id as varchar)
            as dag_run_id,

        cast(lab.ingested_at as timestamp_tz)
            as ingested_at,


        -- ========================================================
        -- DBT AUDIT
        -- ========================================================

        cast(
            current_timestamp()
            as timestamp_ltz
        ) as dbt_loaded_at

    from lab_result as lab

    left join subject_dimension as subject
        on lab.study_id = subject.study_id
       and lab.subject_id = subject.subject_id

)

select *
from final