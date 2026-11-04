-- orders_daily on Redshift (T5): the run's file into landing.orders, as that day's slice.
-- {file} (the S3 file) and {day} (the run's day) are filled in by the DAG. One transaction:
-- a failed load leaves the day as it was. The file is read with the namespace's default
-- role (IAM_ROLE default), never with a person's keys.
CREATE TABLE IF NOT EXISTS landing.orders (
    id BIGINT, customer_id BIGINT, status VARCHAR(20), amount DECIMAL(12, 2), payment_id VARCHAR(64),
    created_at TIMESTAMP, updated_at TIMESTAMP, _source_key VARCHAR(512), _loaded_at TIMESTAMP, dt DATE
);
CREATE TEMP TABLE stage_orders (
    id VARCHAR(32), customer_id VARCHAR(32), status VARCHAR(20), amount VARCHAR(32), payment_id VARCHAR(64),
    created_at VARCHAR(40), updated_at VARCHAR(40)
);
COPY stage_orders FROM '{file}' IAM_ROLE default FORMAT AS CSV IGNOREHEADER 1 EMPTYASNULL;
DELETE FROM landing.orders WHERE dt = '{day}';
INSERT INTO landing.orders
SELECT CAST(id AS BIGINT), CAST(customer_id AS BIGINT), status, CAST(amount AS DECIMAL(12, 2)), payment_id,
       CAST(created_at AS TIMESTAMP), CAST(updated_at AS TIMESTAMP), '{file}', GETDATE(), CAST('{day}' AS DATE)
FROM stage_orders;
