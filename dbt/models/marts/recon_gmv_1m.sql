-- Batch GMV is every payment, including ones the watermark dropped.
-- Streaming GMV is gmv_1m. The gap should be between 0 and late_gmv:
-- some injected late payments still land in an open window (the watermark
-- does not exist on the first batches), and the rest are the undercount.
select
    batch.window_start,
    batch.window_end,
    batch.currency,
    batch.gmv as batch_gmv,
    coalesce(stream.gmv, 0) as streaming_gmv,
    batch.gmv - coalesce(stream.gmv, 0) as gmv_diff,
    coalesce(batch.late_gmv, 0) as late_gmv
from {{ ref('fct_gmv_1m') }} as batch
left join {{ ref('stg_gmv_1m') }} as stream
    on batch.window_start = stream.window_start
    and batch.currency = stream.currency
