{{
    config(
        materialized='table',

        contract={
            "enforced": true
        }
    )
}}

with adverse_event as (

    select *
    from {{ ref('stg_adverse_event') }}

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

        cast(ae.ae_key as varchar)
            as adverse_event_key,


        -- ========================================================
        -- DIMENSION KEYS
        -- ========================================================

        cast(ae.study_key as varchar)
            as study_key,

        cast(subject.site_key as varchar)
            as site_key,

        cast(ae.subject_key as varchar)
            as subject_key,


        -- ========================================================
        -- BUSINESS KEYS
        -- ========================================================

        cast(ae.study_id as varchar)
            as study_id,

        cast(subject.site_id as varchar)
            as site_id,

        cast(ae.subject_id as varchar)
            as subject_id,

        cast(ae.ae_id as varchar)
            as ae_id,


        -- ========================================================
        -- EVENT ATTRIBUTES
        -- ========================================================

        cast(ae.event_term as varchar)
            as event_term,

        cast(ae.severity as varchar)
            as severity,

        cast(ae.serious as varchar)
            as serious,

        cast(ae.processing_priority as varchar)
            as processing_priority,

        cast(ae.requires_safety_review as boolean)
            as requires_safety_review,


        -- ========================================================
        -- EVENT DATES
        -- ========================================================

        cast(ae.event_date as date)
            as event_date,

        cast(ae.reported_date as date)
            as reported_date,


        -- ========================================================
        -- MEASURES
        -- ========================================================

        cast(
            datediff(
                'day',
                ae.event_date,
                ae.reported_date
            )
            as number(38,0)
        ) as reporting_delay_days,

        cast(1 as number(38,0))
            as adverse_event_count,


        -- ========================================================
        -- SOURCE TRACEABILITY
        -- ========================================================

        cast(ae.source_updated_at as timestamp_tz)
            as source_updated_at,

        cast(ae.record_hash as varchar)
            as record_hash,

        cast(ae.source_system as varchar)
            as source_system,

        cast(ae.source_entity as varchar)
            as source_entity,

        cast(ae.dag_run_id as varchar)
            as dag_run_id,

        cast(ae.ingested_at as timestamp_tz)
            as ingested_at,


        -- ========================================================
        -- DBT AUDIT
        -- ========================================================

        cast(
            current_timestamp()
            as timestamp_ltz
        ) as dbt_loaded_at

    from adverse_event as ae

    left join subject_dimension as subject
        on ae.study_id = subject.study_id
       and ae.subject_id = subject.subject_id

)

select *
from final