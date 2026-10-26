-- Delta Live Tables: the shop's orders, cleaned and checked, and the daily report from them.
-- The team's data rules are expectations: Databricks records on every update how many rows
-- broke each one (warn only, nothing is dropped).

CREATE OR REFRESH MATERIALIZED VIEW orders_clean (
  CONSTRAINT valid_amount EXPECT (amount > 0),
  CONSTRAINT known_status EXPECT (status IN ('CREATED', 'PAID', 'SHIPPED', 'CANCELLED')),
  CONSTRAINT paid_has_payment EXPECT (status NOT IN ('PAID', 'SHIPPED') OR payment_id IS NOT NULL)
)
COMMENT 'Assertix lab: the shop orders, with the team rules checked'
AS SELECT
  CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id, CAST(status AS STRING) AS status,
  CAST(order_amount AS DECIMAL(12,2)) AS amount, CAST(payment_id AS STRING) AS payment_id,
  CAST(created_at AS TIMESTAMP) AS created_at, CAST(updated_at AS TIMESTAMP) AS updated_at
FROM shop.public.orders;

CREATE OR REFRESH MATERIALIZED VIEW orders_daily_dlt
COMMENT 'Assertix lab: one row per order day (same figures as orders_daily)'
AS SELECT
  to_date(created_at)                                                 AS order_date,
  count(*)                                                            AS order_count,
  sum(CASE WHEN status IN ('PAID', 'SHIPPED') THEN 1 ELSE 0 END)      AS paid_order_count,
  CAST(sum(amount) AS DECIMAL(22,2))                                  AS total_amount,
  CAST(coalesce(sum(CASE WHEN status IN ('PAID', 'SHIPPED') THEN amount END), 0) AS DECIMAL(22,2)) AS paid_amount,
  sum(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END)               AS cancelled_order_count,
  CAST(coalesce(sum(CASE WHEN status = 'CANCELLED' THEN amount END), 0) AS DECIMAL(22,2)) AS cancelled_amount
FROM orders_clean
GROUP BY to_date(created_at);
