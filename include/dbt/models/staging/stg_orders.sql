select
    id            as order_id,
    customer_id,
    status,
    amount,
    payment_id,
    created_at,
    updated_at,
    created_at::date as order_date
from {{ source('raw', 'orders') }}
