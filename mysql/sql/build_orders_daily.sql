-- orders_daily on MySQL (T6): one row per order day, from the run's slice of landing.orders.
-- The target database (dev, ci or prod) and the run's day are filled in by the job.
-- Rebuilt from scratch each run, so a new figure needs no ALTER TABLE.
DROP TABLE IF EXISTS {target}.orders_daily;
CREATE TABLE {target}.orders_daily (
    order_date DATE PRIMARY KEY, order_count INT, paid_order_count INT, total_amount DECIMAL(14, 2),
    paid_amount DECIMAL(14, 2), cancelled_order_count INT, cancelled_amount DECIMAL(14, 2),
    avg_order_amount DECIMAL(12, 2), max_order_amount DECIMAL(12, 2)
);
INSERT INTO {target}.orders_daily
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
GROUP BY CAST(created_at AS DATE);
