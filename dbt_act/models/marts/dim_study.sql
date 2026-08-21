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
    from {{ ref('stg_study') }}

),

final as (

    select

        -- ========================================================
        -- TECHNICAL KEY
        -- ========================================================

        cast(study_key as varchar)
            as study_key,


        -- ========================================================
        -- BUSINESS KEY
        -- ========================================================

        cast(study_id as varchar)
            as study_id,


        -- ========================================================
        -- STUDY ATTRIBUTES
        -- ========================================================

        cast(study_name as varchar)
            as study_name,

        cast(phase as varchar)
            as phase,

        cast(target_subjects as number(38,0))
            as target_subjects,


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