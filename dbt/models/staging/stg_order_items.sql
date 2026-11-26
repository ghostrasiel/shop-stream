select
    order_id,
    event_id,
    sku,
    product_name,
    category,
    qty,
    unit_price
from {{ source('shop', 'order_items') }}
