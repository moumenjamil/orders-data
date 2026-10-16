"""orders_daily — the Argus data-lab demo pipeline.

source Postgres (shop.orders)
  -> CSV in S3-compatible storage  s3://raw/orders/dt=<run date>/<run id>.csv
  -> warehouse raw.orders (full snapshot, replaced each run): Postgres,
     or Snowflake ORDERS_DB.RAW.ORDERS when dbt_target is "snowflake"
  -> dbt build (models + tests) into the target schema (dev / ci / prod),
     and in the same task dbt's run_results.json (plus build.json with the
     deployed commit) to s3://dbt-artifacts/orders_daily/<run id>/ —
     uploaded also when dbt fails, then the task fails as dbt did

Argus triggers this through the Airflow REST API and may pass
{"dbt_target": "ci"} as run conf, the way a CI job validates a PR.
Credentials come from Airflow Connections (lab_source,
lab_warehouse_loader, lab_snowflake_loader, lab_s3), never from this file.
"""
from __future__ import annotations

import csv
import io
import json
import os
import re
import subprocess
from datetime import datetime

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Param, dag, get_current_context, task

BUCKET = "raw"
ARTIFACTS_BUCKET = "dbt-artifacts"
# Where dbt lives. Defaults: the local lab image. A hosted Airflow (e.g.
# Astronomer) sets these in its image or deployment.
DBT_BIN = os.environ.get("ARGUS_LAB_DBT_BIN", "/home/airflow/dbt-venv/bin/dbt")
DBT_PROJECT_DIR = os.environ.get("ARGUS_LAB_DBT_PROJECT_DIR", "/opt/airflow/dbt")
# The commit this code was deployed from: the deploy (CI) writes it next
# to the DAGs, so a tester can see which code a run used. Absent on the
# local lab (empty).
_COMMIT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".deployed_commit")
DEPLOYED_COMMIT = (open(_COMMIT_FILE).read().strip() if os.path.exists(_COMMIT_FILE) else "")
COLUMNS = ["id", "customer_id", "status", "amount", "payment_id",
           "created_at", "updated_at"]


@dag(
    dag_id="orders_daily",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={"dbt_target": Param("dev", enum=["dev", "ci", "prod", "snowflake"])},
    tags=["argus-data-lab"],
)
def orders_daily():

    @task
    def extract_orders_to_s3() -> str:
        ctx = get_current_context()
        run_date = ctx["dag_run"].run_after.strftime("%Y-%m-%d")
        safe_run_id = re.sub(r"[^A-Za-z0-9_.-]", "_", ctx["run_id"])
        key = f"orders/dt={run_date}/{safe_run_id}.csv"

        src = PostgresHook(postgres_conn_id="lab_source")
        rows = src.get_records(
            f"SELECT {', '.join(COLUMNS)} FROM orders ORDER BY id")

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(COLUMNS)
        writer.writerows(rows)

        s3 = S3Hook(aws_conn_id="lab_s3")
        if not s3.check_for_bucket(BUCKET):
            s3.create_bucket(bucket_name=BUCKET)
        s3.load_string(buf.getvalue(), key=key, bucket_name=BUCKET,
                       replace=True)
        print(f"Extracted {len(rows)} orders to s3://{BUCKET}/{key}")
        return key

    @task
    def load_raw_orders(key: str, params=None) -> int:
        data = S3Hook(aws_conn_id="lab_s3").read_key(
            key=key, bucket_name=BUCKET)
        if params["dbt_target"] == "snowflake":
            return _load_snowflake(data, key)
        wh = PostgresHook(postgres_conn_id="lab_warehouse_loader")
        conn = wh.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE raw.orders")
                copy_sql = (f"COPY raw.orders ({', '.join(COLUMNS)}) "
                            "FROM STDIN WITH (FORMAT csv, HEADER true)")
                if hasattr(cur, "copy_expert"):      # psycopg2 (e.g. the local lab image)
                    cur.copy_expert(copy_sql, io.StringIO(data))
                else:                                # psycopg 3 (e.g. Astro Runtime 3.3)
                    with cur.copy(copy_sql) as copy:
                        copy.write(data)
                cur.execute("UPDATE raw.orders SET _source_key = %s",
                            (f"s3://{BUCKET}/{key}",))
                cur.execute("SELECT count(*) FROM raw.orders")
                loaded = cur.fetchone()[0]
            conn.commit()
        finally:
            conn.close()
        print(f"Loaded {loaded} rows into raw.orders")
        return loaded

    @task
    def dbt_build(params=None) -> str:
        """dbt build, then its results to S3 in the same task.

        One task on purpose: a hosted Airflow may run every task on a
        different worker, so dbt's local target folder only exists here.
        The results are uploaded also when dbt fails (that is when the
        per-test detail matters); the task then fails as dbt did, so a
        failing dbt test still fails the run.
        """
        run_id = get_current_context()["run_id"]
        target_path = f"/tmp/dbt-target/{re.sub(r'[^A-Za-z0-9_.-]', '_', run_id)}"
        proc = subprocess.run(
            [DBT_BIN, "build", "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR,
             "--target", params["dbt_target"], "--target-path", target_path],
            capture_output=True, text=True)
        print(proc.stdout[-20000:])
        if proc.stderr:
            print(proc.stderr[-5000:])

        s3 = S3Hook(aws_conn_id="lab_s3")
        if not s3.check_for_bucket(ARTIFACTS_BUCKET):
            s3.create_bucket(bucket_name=ARTIFACTS_BUCKET)
        prefix = f"orders_daily/{run_id}"
        results = f"{target_path}/run_results.json"
        if os.path.exists(results):
            s3.load_file(results, key=f"{prefix}/run_results.json", bucket_name=ARTIFACTS_BUCKET,
                         replace=True)
            print(f"Uploaded s3://{ARTIFACTS_BUCKET}/{prefix}/run_results.json")
        else:
            print(f"dbt wrote no run_results.json ({results})")
        s3.load_string(json.dumps({"git_sha": DEPLOYED_COMMIT or None, "dbt_target": params["dbt_target"],
                                   "dbt_exit_code": proc.returncode}),
                       key=f"{prefix}/build.json", bucket_name=ARTIFACTS_BUCKET, replace=True)

        if proc.returncode != 0:
            raise RuntimeError(f"dbt build failed (exit code {proc.returncode})")
        if not os.path.exists(results):
            raise FileNotFoundError(f"dbt wrote no run_results.json ({results})")
        return f"{prefix}/run_results.json"

    load_raw_orders(extract_orders_to_s3()) >> dbt_build()


def _load_snowflake(data: str, key: str) -> int:
    """The same snapshot into Snowflake ORDERS_DB.RAW.ORDERS (D7-43).

    Imported here so the local lab (no Snowflake provider) still loads
    the DAG. Empty CSV fields are NULL, as with Postgres COPY.
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    rows = list(csv.reader(io.StringIO(data)))[1:]
    rows = [[v if v != "" else None for v in r] for r in rows]
    conn = SnowflakeHook(snowflake_conn_id="lab_snowflake_loader").get_conn()
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("DELETE FROM ORDERS_DB.RAW.ORDERS")
        if rows:
            cur.executemany(
                f"INSERT INTO ORDERS_DB.RAW.ORDERS ({', '.join(COLUMNS)}, _SOURCE_KEY) "
                f"VALUES ({', '.join(['%s'] * (len(COLUMNS) + 1))})",
                [r + [f"s3://{BUCKET}/{key}"] for r in rows])
        cur.execute("SELECT count(*) FROM ORDERS_DB.RAW.ORDERS")
        loaded = cur.fetchone()[0]
        cur.execute("COMMIT")
    finally:
        conn.close()
    print(f"Loaded {loaded} rows into Snowflake ORDERS_DB.RAW.ORDERS")
    return loaded


orders_daily()
