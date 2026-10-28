"""orders_daily_aws — the orders pipeline on AWS (D7-45).

The same business flow as orders_daily, on AWS services:
source Postgres (shop.orders)
  -> today's snapshot as CSV in S3   s3://<raw bucket>/orders/dt=<run date>/orders.csv
     (the day's folder is replaced: one snapshot per day; times in UTC,
     "YYYY-MM-DD HH:MM:SS", the format the catalog table and the Glue job read)
  -> the team's Great Expectations rules on that file; their result to
     s3://<artifacts bucket>/orders_daily_aws/<run id>/gx_results.json, and a
     broken rule stops the run before anything is curated
  -> the day registered as a partition of orders_lab.raw_orders (Athena DDL)
  -> the Glue job orders-daily-curate for that day: orders_lab.orders_daily
     (Parquet in S3), queried with Athena
  -> build.json next to the GX result: the deployed commit and the Glue run

Credentials come from the Airflow connections lab_source and lab_aws (the
pipeline's AWS login), the bucket names from the Airflow variables
orders_raw_bucket and orders_artifacts_bucket; never from this file.
The Glue job's code is deployed to S3 by the team's CI (.github/workflows).
"""
from __future__ import annotations

import csv
import io
import json
import os
import re
import subprocess
import time
from datetime import datetime, timedelta, timezone

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Variable, dag, get_current_context, task

AWS_CONN = "lab_aws"
REGION = "eu-north-1"
DATABASE = "orders_lab"
GLUE_JOB = "orders-daily-curate"
COLUMNS = ["id", "customer_id", "status", "amount", "payment_id", "created_at", "updated_at"]

GX_PYTHON = os.environ.get("ARGUS_LAB_GX_PYTHON", "/home/airflow/gx-venv/bin/python")
GX_DIR = os.environ.get("ARGUS_LAB_GX_DIR", "/opt/airflow/gx")
_COMMIT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".deployed_commit")
DEPLOYED_COMMIT = (open(_COMMIT_FILE).read().strip() if os.path.exists(_COMMIT_FILE) else "")


def _utc(v):
    """Times as plain UTC text; everything else unchanged (None -> empty field)."""
    if isinstance(v, datetime):
        return (v.astimezone(timezone.utc) if v.tzinfo else v).strftime("%Y-%m-%d %H:%M:%S")
    return v


def _run_folder() -> str:
    return "orders_daily_aws/" + get_current_context()["run_id"]


@dag(
    dag_id="orders_daily_aws",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["argus-data-lab", "aws"],
)
def orders_daily_aws():

    @task
    def extract_orders_to_s3() -> str:
        # the snapshot belongs to the business day that just ended (finance closes the day at midnight)
        run_date = (get_current_context()["dag_run"].run_after - timedelta(days=1)).strftime("%Y-%m-%d")
        raw = Variable.get("orders_raw_bucket")
        rows = PostgresHook(postgres_conn_id="lab_source").get_records(
            f"SELECT {', '.join(COLUMNS)} FROM orders ORDER BY id")
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(COLUMNS)
        writer.writerows([[_utc(v) for v in r] for r in rows])

        s3 = S3Hook(aws_conn_id=AWS_CONN)
        prefix = f"orders/dt={run_date}/"
        old = s3.list_keys(bucket_name=raw, prefix=prefix) or []
        if old:
            s3.delete_objects(bucket=raw, keys=old)
        s3.load_string(buf.getvalue(), key=prefix + "orders.csv", bucket_name=raw, replace=True)
        print(f"Extracted {len(rows)} orders to s3://{raw}/{prefix}orders.csv"
              + (f" (replaced {len(old)} file(s) of that day)" if old else ""))
        return run_date

    @task
    def check_raw_with_gx(run_date: str) -> str:
        """The team's GX rules on the day's file; the result is uploaded also when a rule broke."""
        raw, art = Variable.get("orders_raw_bucket"), Variable.get("orders_artifacts_bucket")
        s3 = S3Hook(aws_conn_id=AWS_CONN)
        work = "/tmp/gx/" + re.sub(r"[^A-Za-z0-9_.-]", "_", get_current_context()["run_id"])
        os.makedirs(work, exist_ok=True)
        with open(f"{work}/orders.csv", "w") as f:
            f.write(s3.read_key(key=f"orders/dt={run_date}/orders.csv", bucket_name=raw))
        proc = subprocess.run(
            [GX_PYTHON, f"{GX_DIR}/check_raw_orders.py", f"{work}/orders.csv", f"{work}/gx_results.json"],
            capture_output=True, text=True, env={**os.environ, "GX_ANALYTICS_ENABLED": "false"})
        print(proc.stdout[-5000:])
        if proc.returncode not in (0, 1):
            print(proc.stderr[-5000:])
        out = f"{_run_folder()}/gx_results.json"
        if os.path.exists(f"{work}/gx_results.json"):
            s3.load_file(f"{work}/gx_results.json", key=out, bucket_name=art, replace=True)
            print(f"Uploaded s3://{art}/{out}")
        if proc.returncode == 1:
            raise RuntimeError("the raw orders broke the team's GX rules: not curated")
        if proc.returncode != 0:
            raise RuntimeError(f"GX could not check the raw orders (exit code {proc.returncode})")
        return run_date

    @task
    def register_partition(run_date: str) -> str:
        """The day's folder as a partition of orders_lab.raw_orders, so Athena sees it."""
        raw, art = Variable.get("orders_raw_bucket"), Variable.get("orders_artifacts_bucket")
        athena = AwsBaseHook(aws_conn_id=AWS_CONN, client_type="athena", region_name=REGION).get_conn()
        qid = athena.start_query_execution(
            QueryString=(f"ALTER TABLE raw_orders ADD IF NOT EXISTS PARTITION (dt='{run_date}') "
                         f"LOCATION 's3://{raw}/orders/dt={run_date}/'"),
            QueryExecutionContext={"Database": DATABASE}, WorkGroup="primary",
            ResultConfiguration={"OutputLocation": f"s3://{art}/athena-results/pipeline/"})["QueryExecutionId"]
        while True:
            st = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
            if st["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(1)
        if st["State"] != "SUCCEEDED":
            raise RuntimeError(f"registering dt={run_date} failed: {st.get('StateChangeReason')}")
        print(f"orders_lab.raw_orders: partition dt={run_date} registered")
        return run_date

    curate = GlueJobOperator(
        task_id="curate_orders_daily",
        job_name=GLUE_JOB,
        script_args={"--run_date": "{{ ti.xcom_pull(task_ids='register_partition') }}",
                     "--airflow_run_id": "{{ run_id }}"},
        aws_conn_id=AWS_CONN,
        region_name=REGION,
        wait_for_completion=True,
        verbose=False,
    )

    @task
    def publish_build_info() -> str:
        """Which code this run used: the deployed commit and the Glue run, next to the GX result."""
        ctx = get_current_context()
        glue_run = ctx["ti"].xcom_pull(task_ids="curate_orders_daily")
        out = f"{_run_folder()}/build.json"
        S3Hook(aws_conn_id=AWS_CONN).load_string(
            json.dumps({"git_sha": DEPLOYED_COMMIT or None, "glue_job": GLUE_JOB, "glue_job_run_id": glue_run}),
            key=out, bucket_name=Variable.get("orders_artifacts_bucket"), replace=True)
        return out

    day = register_partition(check_raw_with_gx(extract_orders_to_s3()))
    day >> curate >> publish_build_info()


orders_daily_aws()
