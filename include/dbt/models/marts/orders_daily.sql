-- Plain SQL that runs on PostgreSQL and Snowflake alike (no FILTER clause).
select
    order_date,
    count(*)                                              as order_count,
    sum(case when status in ('PAID', 'SHIPPED') then 1 else 0 end) as paid_order_count,
    sum(amount)                                           as total_amount,
    coalesce(sum(case when status in ('PAID', 'SHIPPED') then amount end), 0)
                                                          as paid_amount,
    sum(case when status = 'CANCELLED' then 1 else 0 end) as cancelled_order_count,
    coalesce(sum(case when status = 'CANCELLED' then amount end), 0)
                                                          as cancelled_amount
from {{ ref('stg_orders') }}
group by order_date
