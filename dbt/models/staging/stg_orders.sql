select
    order_id,
    event_id,
    event_time,
    user_id,
    session_id,
    currency,
    item_count,
    total_amount
from {{ source('shop', 'orders') }}
