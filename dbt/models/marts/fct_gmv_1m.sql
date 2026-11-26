select
    window_start,
    window_end,
    currency,
    payment_count,
    gmv,
    late_gmv
from {{ ref('int_gmv_batch_1m') }}
