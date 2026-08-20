-- ============================================================
-- MART RECONCILIATION
--
-- Every AE from the operational context must appear
-- exactly once in MART_AE_CASE.
-- ============================================================

with intermediate as (

    select count(*) as row_count

    from {{ ref('int_adverse_event_operational_context') }}

),

mart as (

    select count(*) as row_count

    from {{ ref('mart_ae_case') }}

)

select
    intermediate.row_count
        as intermediate_row_count,

    mart.row_count
        as mart_row_count

from intermediate

cross join mart

where intermediate.row_count <> mart.row_count