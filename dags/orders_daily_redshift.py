"""orders_daily_redshift — the orders pipeline on Amazon Redshift (T5, D7-52).

source Postgres (shop.orders)
  -> CSV in the AWS raw bucket   s3://<raw bucket>/redshift/orders/dt=<run date>/<run id>.csv
  -> Redshift landing.orders: that day's slice replaced (dt), with _loaded_at (UTC),
     loaded by COPY with the namespace's own role (sql/redshift/load_landing_orders.sql)
  -> Redshift <target>.orders_daily rebuilt from the day's slice
     (sql/redshift/build_orders_daily.sql); target dev (default), ci or prod

All SQL runs through the Redshift Data API (workgroup assertix-lab, database orders) as the
pipeline's AWS login (Airflow connection lab_aws): no network path to Redshift, no database
password. Argus triggers this through the Airflow REST API and may pass {"target": "ci"},
the way a CI job validates a PR. A failed statement fails its task with Redshift's own error.
The raw bucket's name comes from the Airflow Variable orders_raw_bucket.
"""
from __future__ import annotations

import csv
import io
import os
import re
import time
from datetime import datetime

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Param, Variable, dag, get_current_context, task

WORKGROUP = "assertix-lab"
DATABASE = "orders"
SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql", "redshift")
COLUMNS = ["id", "customer_id", "status", "amount", "payment_id", "created_at", "updated_at"]
# timestamps as UTC text, the way the load casts them back
EXTRACT = ("SELECT id, customer_id, status, amount, payment_id, "
           "to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US'), "
           "to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') FROM orders WHERE updated_at >= current_date ORDER BY id")
WAIT_SECONDS = 900


def _run_date() -> str:
    return get_current_context()["dag_run"].run_after.strftime("%Y-%m-%d")


def _statements(name: str, **values: str) -> list[str]:
    """A SQL file of the team's, its {placeholders} filled in, split into statements."""
    text = open(os.path.join(SQL_DIR, name)).read()
    for k, v in values.items():
        text = text.replace("{" + k + "}", v)
    lines = [l for l in text.splitlines() if not l.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def _redshift(statements: list[str]) -> None:
    """The statements in one transaction through the Data API; Redshift's own error on failure."""
    client = AwsBaseHook(aws_conn_id="lab_aws", client_type="redshift-data").get_conn()
    sid = client.batch_execute_statement(WorkgroupName=WORKGROUP, Database=DATABASE, Sqls=statements)["Id"]
    deadline = time.monotonic() + WAIT_SECONDS
    while True:
        d = client.describe_statement(Id=sid)
        if d["Status"] in ("FINISHED", "FAILED", "ABORTED"):
            break
        if time.monotonic() > deadline:
            client.cancel_statement(Id=sid)
            raise TimeoutError(f"Redshift did not finish within {WAIT_SECONDS} s (statement {sid})")
        time.sleep(3)
    if d["Status"] != "FINISHED":
        failed = [s for s in d.get("SubStatements", []) if s.get("Status") == "FAILED"]
        why = (failed[0].get("Error") if failed else None) or d.get("Error") or d["Status"]
        raise RuntimeError(f"Redshift: {why}")
    print(f"Redshift ran {len(statements)} statement(s) ({sid})")


@dag(
    dag_id="orders_daily_redshift",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={"target": Param("dev", enum=["dev", "ci", "prod"])},
    tags=["argus-data-lab", "redshift"],
)
def orders_daily_redshift():

    @task
    def extract_orders_to_s3() -> str:
        run_date = _run_date()
        safe_run_id = re.sub(r"[^A-Za-z0-9_.-]", "_", get_current_context()["run_id"])
        bucket = Variable.get("orders_raw_bucket")
        key = f"redshift/orders/dt={run_date}/{safe_run_id}.csv"
        rows = PostgresHook(postgres_conn_id="lab_source").get_records(EXTRACT)
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(COLUMNS)
        writer.writerows(rows)
        S3Hook(aws_conn_id="lab_aws").load_string(buf.getvalue(), key=key, bucket_name=bucket, replace=True)
        print(f"Extracted {len(rows)} orders to s3://{bucket}/{key}")
        return f"s3://{bucket}/{key}"

    @task
    def load_landing_orders(file: str) -> None:
        _redshift(_statements("load_landing_orders.sql", file=file, day=_run_date()))

    @task
    def build_orders_daily(params=None) -> None:
        target = params["target"]
        if target not in ("dev", "ci", "prod"):
            raise ValueError(f"unknown target {target} (dev, ci or prod)")
        _redshift(_statements("build_orders_daily.sql", target=target, day=_run_date()))

    load_landing_orders(extract_orders_to_s3()) >> build_orders_daily()


orders_daily_redshift()
