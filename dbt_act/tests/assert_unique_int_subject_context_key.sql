-- Fail when joining STUDY + SITE + SUBJECT
-- accidentally creates duplicate subject rows.

select
    study_id,
    subject_id,
    count(*) as row_count

from {{ ref('int_subject_context') }}

group by
    study_id,
    subject_id

having count(*) > 1