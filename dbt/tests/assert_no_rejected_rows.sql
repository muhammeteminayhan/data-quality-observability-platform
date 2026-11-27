-- The staging layer should reject nothing when the source is clean.
--
-- This is the test that turns an invisible filter into a visible failure. It
-- is grouped so the output names the cause rather than listing thousands of
-- individual rows.
select
    source_table,
    reject_reason,
    count(*) as rejected_rows
from {{ ref('stg_rejected_rows') }}
group by source_table, reject_reason
