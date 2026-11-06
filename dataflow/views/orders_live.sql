-- orders_live: each order's latest state, from every event the Dataflow job accepted. The team's CI
-- applies it on every deploy ({project}, {dataset} filled in): a PR's to orders_ci, main's to both.
-- A repeated event (same event_id) counts once; the status is the type of the event that happened
-- last (occurred_at), whatever order the events arrived in; details come from the newest event
-- that carries them.
CREATE OR REPLACE VIEW `{project}.{dataset}.orders_live` AS
WITH events AS (
  SELECT * EXCEPT (n) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY received_at) AS n
    FROM `{project}.{dataset}.order_events_live`)
  WHERE n = 1)
SELECT
  order_id,
  ARRAY_AGG(customer_id IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS customer_id,
  ARRAY_AGG(amount IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS amount,
  ARRAY_AGG(type ORDER BY occurred_at DESC, received_at DESC LIMIT 1)[OFFSET(0)] AS status,
  MAX(occurred_at) AS last_event_at,
  MAX(received_at) AS updated_at
FROM events
GROUP BY order_id
