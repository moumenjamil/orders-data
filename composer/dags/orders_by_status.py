"""orders_by_status — the orders rollup on Cloud Composer (Google's managed
Airflow), T2 (D7-49).

BigQuery orders_lab.raw_orders, newest day (dt)
  -> orders_lab.orders_by_status: one row per status of that day, with the
     number of orders and their amount (rebuilt whole on every run)

Runs as the Composer environment's own service account (orders-pipeline):
no keys or connections in this file. Deployed to the environment's DAGs
folder by composer/deploy.sh.
"""
from __future__ import annotations

from datetime import datetime

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sdk import DAG

DATASET = "assertix-lab.orders_lab"
ROLLUP = f"""
CREATE OR REPLACE TABLE `{DATASET}.orders_by_status` AS
SELECT
  dt,
  status,
  COUNT(*)    AS order_count,
  SUM(amount) AS total_amount
FROM `{DATASET}.raw_orders`
WHERE dt = (SELECT MAX(dt) FROM `{DATASET}.raw_orders`)
GROUP BY dt, status
"""

with DAG(
    dag_id="orders_by_status",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["argus-data-lab", "composer"],
):
    BigQueryInsertJobOperator(
        task_id="rollup_by_status",
        location="europe-west3",
        configuration={"query": {"query": ROLLUP, "useLegacySql": False}},
    )
