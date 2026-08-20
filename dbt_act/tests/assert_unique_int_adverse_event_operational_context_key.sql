-- The aggregation and enrichment must not
-- change the adverse-event grain.

select
    study_id,
    ae_id,
    count(*) as row_count

from {{ ref('int_adverse_event_operational_context') }}

group by
    study_id,
    ae_id

having count(*) > 1