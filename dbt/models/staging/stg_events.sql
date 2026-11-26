select
    event_id,
    event_type,
    event_time,
    ingested_at,
    user_id,
    session_id,
    order_id,
    payment_id
from {{ source('shop', 'events') }}
