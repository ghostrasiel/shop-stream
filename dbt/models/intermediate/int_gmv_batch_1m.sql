select
    date_trunc('minute', event_time) as window_start,
    date_trunc('minute', event_time) + interval '1 minute' as window_end,
    'USD'::char(3) as currency,
    count(*) as payment_count,
    sum(amount) as gmv,
    sum(amount) filter (where is_late) as late_gmv
from {{ ref('stg_payments') }}
group by 1, 2, 3
