{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('act_raw', 'study') }}

),

renamed as (

    select

        -- ========================================================
        -- TECHNICAL KEY
        -- ========================================================

        {{ dbt_utils.generate_surrogate_key([
            'study_id'
        ]) }} as study_key,

        -- ========================================================
        -- BUSINESS KEY
        -- ========================================================

        study_id,

        -- ========================================================
        -- STUDY ATTRIBUTES
        -- ========================================================

        study_name,
        phase,
        target_subjects,

        -- ========================================================
        -- SOURCE TIMESTAMP
        -- ========================================================

        updated_at as source_updated_at,

        -- ========================================================
        -- TRACEABILITY
        -- ========================================================

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