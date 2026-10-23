-- Today's full snapshot of the shop's orders, one per day (dt): the day's rows are replaced.
CREATE SCHEMA IF NOT EXISTS workspace.orders_lab;

CREATE TABLE IF NOT EXISTS workspace.orders_lab.raw_orders (
  id BIGINT, customer_id BIGINT, status STRING, amount DECIMAL(12,2), payment_id STRING,
  created_at TIMESTAMP, updated_at TIMESTAMP, dt DATE)
PARTITIONED BY (dt)
COMMENT 'Assertix lab: one full snapshot of the shop orders per day (dt)';

DELETE FROM workspace.orders_lab.raw_orders WHERE dt = current_date();

INSERT INTO workspace.orders_lab.raw_orders
SELECT CAST(id AS BIGINT), CAST(customer_id AS BIGINT), CAST(status AS STRING), CAST(amount AS DECIMAL(12,2)),
       CAST(payment_id AS STRING), CAST(created_at AS TIMESTAMP), CAST(updated_at AS TIMESTAMP), current_date()
FROM shop.public.orders
-- Earlier changes are already in earlier snapshots: read only today's changes.
WHERE updated_at >= current_date();
