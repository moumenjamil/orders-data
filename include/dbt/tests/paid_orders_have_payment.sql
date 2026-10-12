-- Business rule: a paid order must have a payment_id.
-- Returns the violating rows; dbt fails the test if any exist.
select order_id, status, payment_id
from {{ ref('stg_orders') }}
where status in ('PAID', 'SHIPPED')
  and payment_id is null
