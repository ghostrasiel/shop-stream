-- Streaming GMV may be lower than batch GMV, but not by more than the
-- payments whose event_time is more than 2 minutes behind ingested_at.
-- A negative diff would mean the stream counted money the batch cannot see.
select
    window_start,
    batch_gmv,
    streaming_gmv,
    gmv_diff,
    late_gmv
from {{ ref('recon_gmv_1m') }}
where gmv_diff < -0.01
   or gmv_diff > late_gmv + 0.01
