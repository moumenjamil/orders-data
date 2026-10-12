select
    order_date,
    count(*)                                              as order_count,
    count(*) filter (where status in ('PAID', 'SHIPPED')) as paid_order_count,
    sum(amount)                                           as total_amount,
    coalesce(sum(amount) filter (where status in ('PAID', 'SHIPPED')), 0)
                                                          as paid_amount
from {{ ref('stg_orders') }}
group by order_date
