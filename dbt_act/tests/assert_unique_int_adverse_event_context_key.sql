-- Fail when enrichment creates more than one row
-- for the same STUDY_ID + AE_ID.

select
    study_id,
    ae_id,
    count(*) as row_count

from {{ ref('int_adverse_event_context') }}

group by
    study_id,
    ae_id

having count(*) > 1