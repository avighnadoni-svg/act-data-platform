{{
    config(
        materialized='table',

        contract={
            "enforced": true
        }
    )
}}

with data_query as (

    select *
    from {{ ref('stg_data_query') }}

),

final as (

    select

        -- ========================================================
        -- FACT KEY
        -- ========================================================

        cast(data_query_key as varchar)
            as data_query_key,


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

        cast(query_id as varchar)
            as query_id,

        cast(site_id as varchar)
            as site_id,

        cast(subject_id as varchar)
            as subject_id,


        -- ========================================================
        -- QUERY ATTRIBUTES
        -- ========================================================

        cast(status as varchar)
            as status,

        cast(opened_date as date)
            as opened_date,

        cast(resolved_date as date)
            as resolved_date,


        -- ========================================================
        -- MEASURES
        -- ========================================================

        cast(
            case
                when resolved_date is not null
                then datediff(
                    'day',
                    opened_date,
                    resolved_date
                )
                else null
            end
            as number(38,0)
        ) as resolution_days,

        cast(1 as number(38,0))
            as data_query_count,

        cast(
            case
                when resolved_date is null then 1
                else 0
            end
            as number(38,0)
        ) as open_query_count,

        cast(
            case
                when resolved_date is not null then 1
                else 0
            end
            as number(38,0)
        ) as resolved_query_count,


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

    from data_query

)

select *
from final