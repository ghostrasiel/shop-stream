select
    date_trunc('day', window_start) as day,
    currency,
    sum(payment_count) as payment_count,
    sum(gmv) as gmv,
    sum(late_gmv) as late_gmv
from {{ ref('fct_gmv_1m') }}
group by 1, 2
