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
    from {{ ref('stg_site') }}

),

final as (

    select

        -- ========================================================
        -- TECHNICAL KEYS
        -- ========================================================

        cast(site_key as varchar)
            as site_key,

        cast(study_key as varchar)
            as study_key,


        -- ========================================================
        -- BUSINESS KEYS
        -- ========================================================

        cast(study_id as varchar)
            as study_id,

        cast(site_id as varchar)
            as site_id,


        -- ========================================================
        -- SITE ATTRIBUTES
        -- ========================================================

        cast(country as varchar)
            as country,

        cast(investigator as varchar)
            as investigator,

        cast(target_enrollment as number(38,0))
            as target_enrollment,


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