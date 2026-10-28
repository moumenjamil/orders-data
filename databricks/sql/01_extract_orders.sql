-- Today's full snapshot of the shop's orders, one per day (dt): the day's rows are replaced.
CREATE SCHEMA IF NOT EXISTS workspace.orders_lab;

CREATE TABLE IF NOT EXISTS workspace.orders_lab.raw_orders (
  id BIGINT, customer_id BIGINT, status STRING, amount DECIMAL(12,2), payment_id STRING,
  created_at TIMESTAMP, updated_at TIMESTAMP, dt DATE,
  loaded_at TIMESTAMP COMMENT 'when this row was loaded (UTC)')
PARTITIONED BY (dt)
COMMENT 'Assertix lab: one full snapshot of the shop orders per day (dt)';

-- the snapshot belongs to the business day that just ended (finance closes the day at midnight)
DELETE FROM workspace.orders_lab.raw_orders WHERE dt = date_sub(current_date(), 1);

INSERT INTO workspace.orders_lab.raw_orders
  (id, customer_id, status, amount, payment_id, created_at, updated_at, dt, loaded_at)
SELECT CAST(id AS BIGINT), CAST(customer_id AS BIGINT), CAST(status AS STRING), CAST(amount AS DECIMAL(12,2)),
       CAST(payment_id AS STRING), CAST(created_at AS TIMESTAMP), CAST(updated_at AS TIMESTAMP), date_sub(current_date(), 1),
       current_timestamp()
FROM shop.public.orders;
