{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('act_raw', 'protocol_deviation') }}

),

renamed as (

    select

        -- ========================================================
        -- TECHNICAL KEYS
        -- ========================================================

        {{ act_surrogate_key([
            'study_id',
            'deviation_id'
        ]) }} as protocol_deviation_key,

        {{ act_surrogate_key([
            'study_id'
        ]) }} as study_key,

        case
            when subject_id is not null
            then {{ act_surrogate_key([
                'study_id',
                'subject_id'
            ]) }}
        end as subject_key,

        case
            when site_id is not null
            then {{ act_surrogate_key([
                'study_id',
                'site_id'
            ]) }}
        end as site_key,

        -- ========================================================
        -- BUSINESS KEYS
        -- ========================================================

        study_id,
        deviation_id,

        subject_id,
        site_id,

        -- ========================================================
        -- DEVIATION ATTRIBUTES
        -- ========================================================

        deviation_type,
        severity,

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