{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('act_raw', 'site') }}

),

renamed as (

    select

        -- ========================================================
        -- TECHNICAL KEYS
        -- ========================================================

        {{ dbt_utils.generate_surrogate_key([
            'study_id',
            'site_id'
        ]) }} as site_key,

        {{ dbt_utils.generate_surrogate_key([
            'study_id'
        ]) }} as study_key,

        -- ========================================================
        -- BUSINESS KEYS
        -- ========================================================

        study_id,
        site_id,

        -- ========================================================
        -- SITE ATTRIBUTES
        -- ========================================================

        country,
        investigator,
        target_enrollment,

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