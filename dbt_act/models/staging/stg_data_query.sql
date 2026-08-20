{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('act_raw', 'data_query') }}

),

renamed as (

    select

        -- ========================================================
        -- TECHNICAL KEYS
        -- ========================================================

        {{ act_surrogate_key([
            'study_id',
            'query_id'
        ]) }} as data_query_key,

        {{ act_surrogate_key([
            'study_id',
            'subject_id'
        ]) }} as subject_key,

        {{ act_surrogate_key([
            'study_id',
            'site_id'
        ]) }} as site_key,

        {{ act_surrogate_key([
            'study_id'
        ]) }} as study_key,

        -- ========================================================
        -- BUSINESS KEYS
        -- ========================================================

        study_id,
        query_id,

        subject_id,
        site_id,

        -- ========================================================
        -- QUERY ATTRIBUTES
        -- ========================================================

        opened_date,
        resolved_date,
        status,

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