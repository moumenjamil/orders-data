-- cast(... as date): the same on PostgreSQL, Snowflake and BigQuery.
-- On BigQuery the raw table keeps one slice per day: read the run's day.
select
    id            as order_id,
    customer_id,
    status,
    amount,
    payment_id,
    created_at,
    updated_at,
    cast(created_at as date) as order_date
from {{ source('raw', 'orders') }}
-- cancelled orders are not revenue: leave them out of staging
where status != 'CANCELLED'
{% if target.type == 'bigquery' %}
  and dt = '{{ var("run_date") }}'
{% endif %}
