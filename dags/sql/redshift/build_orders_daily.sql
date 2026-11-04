-- orders_daily on Redshift (T5): one row per order day, from the run's slice of landing.orders.
-- The target schema (dev, ci or prod) and the run's day are filled in by the DAG.
-- Rebuilt whole in one transaction (CREATE TABLE AS), so a failed run leaves the old table;
-- the columns follow the SELECT, so a new figure needs no ALTER TABLE.
DROP TABLE IF EXISTS {target}.orders_daily;
CREATE TABLE {target}.orders_daily AS
SELECT
    CAST(created_at AS DATE)                                       AS order_date,
    COUNT(*)                                                       AS order_count,
    SUM(CASE WHEN status IN ('PAID', 'SHIPPED') THEN 1 ELSE 0 END) AS paid_order_count,
    SUM(amount)                                                    AS total_amount,
    COALESCE(SUM(CASE WHEN status IN ('PAID', 'SHIPPED') THEN amount END), 0) AS paid_amount,
    SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END)          AS cancelled_order_count,
    COALESCE(SUM(CASE WHEN status = 'CANCELLED' THEN amount END), 0) AS cancelled_amount,
    ROUND(AVG(amount), 2)                                          AS avg_order_amount,
    MAX(amount)                                                    AS max_order_amount
FROM landing.orders
WHERE dt = '{day}'
  AND status <> 'CANCELLED'   -- cancelled orders are not revenue
GROUP BY CAST(created_at AS DATE);
