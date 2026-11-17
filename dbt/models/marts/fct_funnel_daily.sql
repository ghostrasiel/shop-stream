select
    date_trunc('day', event_time) as day,
    count(*) filter (where event_type = 'page_view') as page_views,
    count(*) filter (where event_type = 'add_to_cart') as add_to_carts,
    count(*) filter (where event_type = 'order_created') as orders,
    count(*) filter (where event_type = 'payment_completed') as payments
from {{ ref('stg_events') }}
group by 1
