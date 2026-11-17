select
    payment_id,
    order_id,
    event_id,
    event_time,
    ingested_at,
    user_id,
    session_id,
    amount,
    method,
    status,
    event_time < ingested_at - interval '2 minutes' as is_late
from {{ source('shop', 'payments') }}
