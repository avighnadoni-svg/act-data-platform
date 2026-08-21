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
-- DATA QUERY METRICS
-- ============================================================

query_summary as (

    select

        site_key,

        sum(data_query_count)
            as data_query_count,

        sum(open_query_count)
            as open_query_count,

        sum(resolved_query_count)
            as resolved_query_count,

        count(
            distinct case
                when open_query_count = 1
                then subject_key
            end
        ) as subjects_with_open_queries,

        avg(
            resolution_days
        ) as avg_query_resolution_days,

        max(
            resolution_days
        ) as max_query_resolution_days,

        max(
            case
                when open_query_count = 1
                then datediff(
                    'day',
                    opened_date,
                    current_date()
                )
            end
        ) as oldest_open_query_days

    from {{ ref('fct_data_query') }}

    group by site_key

),

-- ============================================================
-- PROTOCOL DEVIATION METRICS
-- ============================================================

deviation_summary as (

    select

        site_key,

        sum(protocol_deviation_count)
            as protocol_deviation_count,

        count_if(
            upper(severity) = 'MINOR'
        ) as minor_deviation_count,

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
-- FINAL MART
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
        -- DATA QUERY METRICS
        -- ========================================================

        cast(
            coalesce(
                query.data_query_count,
                0
            )
            as number(38,0)
        ) as data_query_count,

        cast(
            coalesce(
                query.open_query_count,
                0
            )
            as number(38,0)
        ) as open_query_count,

        cast(
            coalesce(
                query.resolved_query_count,
                0
            )
            as number(38,0)
        ) as resolved_query_count,

        cast(
            coalesce(
                query.subjects_with_open_queries,
                0
            )
            as number(38,0)
        ) as subjects_with_open_queries,

        cast(
            query.avg_query_resolution_days
            as number(10,2)
        ) as avg_query_resolution_days,

        cast(
            query.max_query_resolution_days
            as number(38,0)
        ) as max_query_resolution_days,

        cast(
            query.oldest_open_query_days
            as number(38,0)
        ) as oldest_open_query_days,


        -- ========================================================
        -- PROTOCOL DEVIATION METRICS
        -- ========================================================

        cast(
            coalesce(
                deviation.protocol_deviation_count,
                0
            )
            as number(38,0)
        ) as protocol_deviation_count,

        cast(
            coalesce(
                deviation.minor_deviation_count,
                0
            )
            as number(38,0)
        ) as minor_deviation_count,

        cast(
            coalesce(
                deviation.major_deviation_count,
                0
            )
            as number(38,0)
        ) as major_deviation_count,

        cast(
            coalesce(
                deviation.critical_deviation_count,
                0
            )
            as number(38,0)
        ) as critical_deviation_count,


        -- ========================================================
        -- OPERATIONAL FLAGS
        -- ========================================================

        cast(
            case
                when coalesce(
                    query.open_query_count,
                    0
                ) > 0
                then true
                else false
            end
            as boolean
        ) as has_open_queries,

        cast(
            case
                when coalesce(
                    deviation.major_deviation_count,
                    0
                ) > 0
                or coalesce(
                    deviation.critical_deviation_count,
                    0
                ) > 0
                then true
                else false
            end
            as boolean
        ) as has_significant_deviations,

        cast(
            case
                when coalesce(
                    query.open_query_count,
                    0
                ) > 0
                or coalesce(
                    deviation.major_deviation_count,
                    0
                ) > 0
                or coalesce(
                    deviation.critical_deviation_count,
                    0
                ) > 0
                then true
                else false
            end
            as boolean
        ) as requires_data_quality_attention,


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

    left join query_summary as query
        on site.site_key = query.site_key

    left join deviation_summary as deviation
        on site.site_key = deviation.site_key

)

select *
from final