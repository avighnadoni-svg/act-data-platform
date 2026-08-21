{{
    config(
        materialized='table',

        contract={
            "enforced": true
        }
    )
}}

with protocol_deviation as (

    select *
    from {{ ref('stg_protocol_deviation') }}

),

final as (

    select

        -- ========================================================
        -- FACT KEY
        -- ========================================================

        cast(protocol_deviation_key as varchar)
            as protocol_deviation_key,


        -- ========================================================
        -- DIMENSION KEYS
        -- ========================================================

        cast(study_key as varchar)
            as study_key,

        cast(site_key as varchar)
            as site_key,

        cast(subject_key as varchar)
            as subject_key,


        -- ========================================================
        -- BUSINESS KEYS
        -- ========================================================

        cast(study_id as varchar)
            as study_id,

        cast(deviation_id as varchar)
            as deviation_id,

        cast(site_id as varchar)
            as site_id,

        cast(subject_id as varchar)
            as subject_id,


        -- ========================================================
        -- DEVIATION ATTRIBUTES
        -- ========================================================

        cast(deviation_type as varchar)
            as deviation_type,

        cast(severity as varchar)
            as severity,


        -- ========================================================
        -- MEASURES
        -- ========================================================

        cast(1 as number(38,0))
            as protocol_deviation_count,


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

    from protocol_deviation

)

select *
from final