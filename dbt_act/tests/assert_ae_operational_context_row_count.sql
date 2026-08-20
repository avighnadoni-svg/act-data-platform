-- Every adverse event entering the operational-context model
-- must still exist after enrichment.
--
-- No AE should be lost and no AE should be multiplied.

with source_count as (

    select count(*) as row_count
    from {{ ref('int_adverse_event_context') }}

),

target_count as (

    select count(*) as row_count
    from {{ ref('int_adverse_event_operational_context') }}

)

select
    source_count.row_count as source_row_count,
    target_count.row_count as target_row_count

from source_count
cross join target_count

where source_count.row_count <> target_count.row_count