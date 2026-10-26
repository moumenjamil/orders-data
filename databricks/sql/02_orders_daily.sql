-- The daily orders report from the latest snapshot (same figures as the dbt model
-- marts/orders_daily and the Glue job orders-daily-curate).
CREATE OR REPLACE TABLE workspace.orders_lab.orders_daily
COMMENT 'Assertix lab: one row per order day, from the latest raw snapshot'
AS
SELECT
  to_date(created_at)                                                 AS order_date,
  count(*)                                                            AS order_count,
  sum(CASE WHEN status IN ('PAID', 'SHIPPED') THEN 1 ELSE 0 END)      AS paid_order_count,
  CAST(sum(amount) AS DECIMAL(22,2))                                  AS total_amount,
  CAST(coalesce(sum(CASE WHEN status IN ('PAID', 'SHIPPED') THEN amount END), 0) AS DECIMAL(22,2)) AS paid_amount,
  sum(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END)               AS cancelled_order_count,
  CAST(coalesce(sum(CASE WHEN status = 'CANCELLED' THEN amount END), 0) AS DECIMAL(22,2)) AS cancelled_amount,
  CAST(avg(amount) AS DECIMAL(22,2))                                  AS avg_order_amount
FROM workspace.orders_lab.raw_orders
WHERE dt = (SELECT max(dt) FROM workspace.orders_lab.raw_orders)
GROUP BY to_date(created_at);
