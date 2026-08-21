{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('act_raw', 'adverse_event') }}

),

renamed as (

    select

        -- ========================================================
        -- TECHNICAL KEYS
        -- ========================================================

        {{ act_surrogate_key([
            'study_id',
            'ae_id'
        ]) }} as ae_key,

        {{ act_surrogate_key([
            'study_id',
            'subject_id'
        ]) }} as subject_key,

        {{ act_surrogate_key([
            'study_id'
        ]) }} as study_key,

        -- ========================================================
        -- BUSINESS KEYS
        -- ========================================================

        study_id,
        ae_id,
        subject_id,

        -- ========================================================
        -- AE ATTRIBUTES
        -- ========================================================

        event_term,
        severity,
        serious,

        event_date,
        reported_date,

        processing_priority,
        requires_safety_review,

        -- ========================================================
        -- SOURCE / TRACEABILITY
        -- ========================================================

        updated_at as source_updated_at,

        record_hash,

        source_system,
        source_entity,
        dag_run_id,
        ingested_at,

        source_file_name,
        source_file_row_number,
        source_file_content_key,
        source_file_last_modified,
        snowflake_load_ts,

        current_row_updated_at

    from source

)

select *
from renamed