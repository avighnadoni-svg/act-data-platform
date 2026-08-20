{{
    config(
        materialized='incremental',

        incremental_strategy='merge',

        unique_key='ae_case_key',

        on_schema_change='fail',

        merge_exclude_columns=[
            'dbt_created_at'
        ],

        contract={
            "enforced": true
        }
    )
}}

with source as (

    select *
    from {{ ref('int_adverse_event_operational_context') }}

    -- ============================================================
    -- INCREMENTAL FILTER
    --
    -- FULL REFRESH:
    --     process all rows
    --
    -- INCREMENTAL:
    --     process only new/changed operational contexts
    --
    -- 5-second overlap protects boundary records.
    -- ============================================================

    {% if is_incremental() %}

        where operational_context_updated_at >= (

            select

                dateadd(
                    second,
                    -5,

                    coalesce(
                        max(operational_context_updated_at),

                        cast(
                            '1900-01-01 00:00:00 +00:00'
                            as timestamp_tz
                        )
                    )
                )

            from {{ this }}

        )

    {% endif %}

),

final as (

    select

        -- ========================================================
        -- TECHNICAL CASE KEY
        -- ========================================================

        cast(
            {{ act_surrogate_key([
                'study_id',
                'ae_id'
            ]) }}
            as varchar
        ) as ae_case_key,


        -- ========================================================
        -- BUSINESS KEYS
        -- ========================================================

        cast(study_id as varchar)
            as study_id,

        cast(ae_id as varchar)
            as ae_id,

        cast(subject_id as varchar)
            as subject_id,

        cast(site_id as varchar)
            as site_id,


        -- ========================================================
        -- STUDY CONTEXT
        -- ========================================================

        cast(study_name as varchar)
            as study_name,

        cast(phase as varchar)
            as phase,

        cast(target_subjects as number(38,0))
            as target_subjects,


        -- ========================================================
        -- SITE CONTEXT
        -- ========================================================

        cast(country as varchar)
            as country,

        cast(investigator as varchar)
            as investigator,

        cast(target_enrollment as number(38,0))
            as target_enrollment,


        -- ========================================================
        -- SUBJECT CONTEXT
        -- ========================================================

        cast(gender as varchar)
            as gender,

        cast(age as number(38,0))
            as age,

        cast(subject_status as varchar)
            as subject_status,

        cast(enrollment_date as date)
            as enrollment_date,


        -- ========================================================
        -- ADVERSE EVENT
        -- ========================================================

        cast(event_term as varchar)
            as event_term,

        cast(severity as varchar)
            as severity,

        cast(serious as varchar)
            as serious,

        cast(event_date as date)
            as event_date,

        cast(reported_date as date)
            as reported_date,

        cast(processing_priority as varchar)
            as processing_priority,

        cast(requires_safety_review as boolean)
            as requires_safety_review,


        -- ========================================================
        -- REPORTING METRICS
        -- ========================================================

        cast(
            datediff(
                'day',
                event_date,
                reported_date
            )
            as number(38,0)
        ) as reporting_delay_days,


        -- ========================================================
        -- VISIT CONTEXT
        -- ========================================================

        cast(visit_count as number(38,0))
            as visit_count,

        cast(latest_actual_visit_date as date)
            as latest_actual_visit_date,

        cast(latest_planned_visit_date as date)
            as latest_planned_visit_date,


        -- ========================================================
        -- LAB CONTEXT
        -- ========================================================

        cast(lab_result_count as number(38,0))
            as lab_result_count,

        cast(abnormal_lab_count as number(38,0))
            as abnormal_lab_count,

        cast(
            abnormal_lab_count > 0
            as boolean
        ) as has_abnormal_lab,


        -- ========================================================
        -- PROTOCOL DEVIATION CONTEXT
        -- ========================================================

        cast(protocol_deviation_count as number(38,0))
            as protocol_deviation_count,

        cast(major_deviation_count as number(38,0))
            as major_deviation_count,

        cast(critical_deviation_count as number(38,0))
            as critical_deviation_count,

        cast(
            protocol_deviation_count > 0
            as boolean
        ) as has_protocol_deviation,

        cast(
            critical_deviation_count > 0
            as boolean
        ) as has_critical_deviation,


        -- ========================================================
        -- DATA QUERY CONTEXT
        -- ========================================================

        cast(data_query_count as number(38,0))
            as data_query_count,

        cast(unresolved_query_count as number(38,0))
            as unresolved_query_count,

        cast(
            unresolved_query_count > 0
            as boolean
        ) as has_unresolved_query,


        -- ========================================================
        -- INCREMENTAL WATERMARK
        -- ========================================================

        cast(
            operational_context_updated_at
            as timestamp_tz
        ) as operational_context_updated_at,


        -- ========================================================
        -- SOURCE TRACEABILITY
        -- ========================================================

        cast(ae_source_updated_at as timestamp_tz)
            as ae_source_updated_at,

        cast(subject_source_updated_at as timestamp_tz)
            as subject_source_updated_at,

        cast(site_source_updated_at as timestamp_tz)
            as site_source_updated_at,

        cast(study_source_updated_at as timestamp_tz)
            as study_source_updated_at,

        cast(record_hash as varchar)
            as record_hash,

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
        ) as dbt_created_at,

        cast(
            current_timestamp()
            as timestamp_ltz
        ) as dbt_updated_at

    from source

)

select *
from final