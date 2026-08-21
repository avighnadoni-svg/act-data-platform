{{
    config(
        materialized='table',

        contract={
            "enforced": true
        }
    )
}}

with source as (

    select *
    from {{ ref('stg_visit') }}

),

final as (

    select

        -- ========================================================
        -- TECHNICAL KEYS
        -- ========================================================

        cast(visit_key as varchar)
            as visit_key,

        cast(subject_key as varchar)
            as subject_key,

        cast(study_key as varchar)
            as study_key,


        -- ========================================================
        -- BUSINESS KEYS
        -- ========================================================

        cast(study_id as varchar)
            as study_id,

        cast(visit_id as varchar)
            as visit_id,

        cast(subject_id as varchar)
            as subject_id,


        -- ========================================================
        -- VISIT ATTRIBUTES
        -- ========================================================

        cast(visit_name as varchar)
            as visit_name,

        cast(planned_date as date)
            as planned_date,

        cast(actual_date as date)
            as actual_date,


        -- ========================================================
        -- SOURCE TRACEABILITY
        -- ========================================================

        cast(source_updated_at as timestamp_tz)
            as source_updated_at,

        cast(record_hash as varchar)
            as record_hash,

        cast(source_system as varchar)
            as source_system,

        cast(source_entity as varchar)
            as source_entity,

        cast(dag_run_id as varchar)
            as dag_run_id,

        cast(ingested_at as timestamp_tz)
            as ingested_at,


        -- ========================================================
        -- DBT AUDIT
        -- ========================================================

        cast(
            current_timestamp()
            as timestamp_ltz
        ) as dbt_loaded_at

    from source

)

select *
from final