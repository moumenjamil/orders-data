"""orders_daily_dbx — Airflow runs the orders pipeline on Databricks (T1b, D7-48).

The work stays in Databricks: the job orders_daily_dbx (databricks/, deployed by
the team's CI as a bundle) copies today's shop orders into
workspace.orders_lab.raw_orders and builds orders_daily. Airflow owns the
schedule and starts the job, as teams do that keep all their schedules in
Airflow. A failed Databricks task fails this Airflow task with Databricks'
errors.

Credentials come from the Airflow connection lab_databricks (the pipeline's own
service principal, OAuth); never from this file.
"""
from __future__ import annotations

from datetime import datetime

from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.sdk import dag

DATABRICKS_CONN = "lab_databricks"
JOB = "orders_daily_dbx"


@dag(
    dag_id="orders_daily_dbx",
    schedule=None,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["argus-data-lab", "databricks"],
)
def orders_daily_dbx():
    DatabricksRunNowOperator(
        task_id="run_orders_daily_dbx",
        databricks_conn_id=DATABRICKS_CONN,
        job_name=JOB,
        polling_period_seconds=15,
    )


orders_daily_dbx()
