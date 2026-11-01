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
    cast(created_at as date) as order_date,
    updated_at::date as last_change_date
from {{ source('raw', 'orders') }}
{% if target.type == 'bigquery' %}
where dt = '{{ var("run_date") }}'
{% endif %}
