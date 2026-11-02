-- cast(... as date): the same on PostgreSQL, Snowflake and BigQuery.
-- On BigQuery the raw table keeps one slice per day: read the run's day
-- (Airflow passes run_date; a dbt Cloud job does not: the day the run started, UTC).
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
-- only orders with a payment count for finance
where payment_id is not null
{% if target.type == 'bigquery' %}
  and dt = '{{ var("run_date", run_started_at.strftime("%Y-%m-%d")) }}'
{% endif %}
