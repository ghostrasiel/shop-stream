select
    window_start,
    window_end,
    currency,
    payment_count,
    gmv
from {{ source('shop', 'gmv_1m') }}
