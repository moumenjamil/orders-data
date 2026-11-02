-- orders_daily on Azure (T4): one row per order day, from the run's slice of raw.orders.
-- The target schema (dev, ci or prod) and the run's day are filled in by the pipeline.
-- Rebuilt whole in one transaction, so a failed run leaves the old table.
SET XACT_ABORT ON;
IF '{target}' NOT IN ('dev', 'ci', 'prod') THROW 50001, 'unknown target {target} (dev, ci or prod)', 1;

BEGIN TRANSACTION;
DELETE FROM {target}.orders_daily;
INSERT INTO {target}.orders_daily (order_date, order_count, paid_order_count, total_amount,
                                   paid_amount, cancelled_order_count, cancelled_amount, avg_order_amount)
SELECT
    CAST(created_at AS DATE)                                       AS order_date,
    COUNT(*)                                                       AS order_count,
    SUM(CASE WHEN status IN ('PAID', 'SHIPPED') THEN 1 ELSE 0 END) AS paid_order_count,
    SUM(amount)                                                    AS total_amount,
    COALESCE(SUM(CASE WHEN status IN ('PAID', 'SHIPPED') THEN amount END), 0) AS paid_amount,
    SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END)          AS cancelled_order_count,
    COALESCE(SUM(CASE WHEN status = 'CANCELLED' THEN amount END), 0) AS cancelled_amount,
    ROUND(AVG(amount), 2)                                          AS avg_order_amount
FROM raw.orders
WHERE dt = '{day}'
GROUP BY CAST(created_at AS DATE);
COMMIT;
