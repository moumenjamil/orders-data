"""orders_daily — the Argus data-lab demo pipeline.

source Postgres (shop.orders)
  -> CSV in S3-compatible storage  s3://raw/orders/dt=<run date>/<run id>.csv
  -> the team's Great Expectations rules on that file (D7-44); their result
     to s3://dbt-artifacts/orders_daily/<run id>/gx_results.json (GCS for
     "bigquery"), and a
     broken rule stops the run before anything is loaded
  -> warehouse raw.orders (full snapshot, replaced each run): Postgres,
     or Snowflake ORDERS_DB.RAW.ORDERS when dbt_target is "snowflake",
     or BigQuery orders_lab.raw_orders when dbt_target is "bigquery" (T2,
     D7-49): the file copied to gs://<raw bucket>/orders/dt=<run date>/,
     then loaded as that day's slice (dt), replaced on a re-run, with
     _loaded_at; dbt then reads only the run's day (var run_date)
  -> dbt build (models + tests) into the target schema (dev / ci / prod),
     and in the same task dbt's run_results.json (plus build.json with the
     deployed commit) to s3://dbt-artifacts/orders_daily/<run id>/ —
     uploaded also when dbt fails, then the task fails as dbt did;
     for "bigquery" to gs://<artifacts bucket>/orders_daily/<run id>/

Argus triggers this through the Airflow REST API and may pass
{"dbt_target": "ci"} as run conf, the way a CI job validates a PR.
Credentials come from Airflow Connections (lab_source,
lab_warehouse_loader, lab_snowflake_loader, lab_s3, lab_gcp), never from
this file; the GCS bucket names from the Airflow Variables
orders_gcs_raw_bucket and orders_gcs_artifacts_bucket.
"""
from __future__ import annotations

import csv
import io
import json
import os
import re
import subprocess
from datetime import datetime, timedelta

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Param, Variable, dag, get_current_context, task

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
# Great Expectations in its own virtualenv (like dbt), and the team's rules.
GX_PYTHON = os.environ.get("ARGUS_LAB_GX_PYTHON", "/home/airflow/gx-venv/bin/python")
GX_DIR = os.environ.get("ARGUS_LAB_GX_DIR", "/opt/airflow/gx")
COLUMNS = ["id", "customer_id", "status", "amount", "payment_id",
           "created_at", "updated_at"]
# BigQuery (T2): the dataset dbt builds into and where its raw table lives
BQ_DATASET = "orders_lab"
BQ_LOCATION = "europe-west3"


@dag(
    dag_id="orders_daily",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={"dbt_target": Param("dev", enum=["dev", "ci", "prod", "snowflake", "bigquery"])},
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
    def check_raw_with_gx(key: str, params=None) -> str:
        """The team's GX rules on the raw file, before it is loaded.

        The result goes to the artifacts bucket next to dbt's (GCS for
        "bigquery"), also when a rule broke (that is when it matters); the
        task then fails, so the load never runs on bad data.
        """
        run_id = get_current_context()["run_id"]
        s3 = S3Hook(aws_conn_id="lab_s3")
        work = f"/tmp/gx/{re.sub(r'[^A-Za-z0-9_.-]', '_', run_id)}"
        os.makedirs(work, exist_ok=True)
        with open(f"{work}/orders.csv", "w") as f:
            f.write(s3.read_key(key=key, bucket_name=BUCKET))
        proc = subprocess.run(
            [GX_PYTHON, f"{GX_DIR}/check_raw_orders.py", f"{work}/orders.csv", f"{work}/gx_results.json"],
            capture_output=True, text=True, env={**os.environ, "GX_ANALYTICS_ENABLED": "false"})
        print(proc.stdout[-5000:])
        if proc.returncode not in (0, 1):
            print(proc.stderr[-5000:])
        out = f"orders_daily/{run_id}/gx_results.json"
        if os.path.exists(f"{work}/gx_results.json"):
            put_file, _, where = _artifacts(params["dbt_target"])
            put_file(f"{work}/gx_results.json", out)
            print(f"Uploaded {where}/{out}")
        if proc.returncode == 1:
            raise RuntimeError("the raw orders broke the team's GX rules: not loaded")
        if proc.returncode != 0:
            raise RuntimeError(f"GX could not check the raw orders (exit code {proc.returncode})")
        return out

    @task
    def load_raw_orders(key: str, params=None) -> int:
        data = S3Hook(aws_conn_id="lab_s3").read_key(
            key=key, bucket_name=BUCKET)
        if params["dbt_target"] == "snowflake":
            return _load_snowflake(data, key)
        if params["dbt_target"] == "bigquery":
            return _load_bigquery(data, key)
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
        ctx = get_current_context()
        run_id = ctx["run_id"]
        run_date = ctx["dag_run"].run_after.strftime("%Y-%m-%d")
        target_path = f"/tmp/dbt-target/{re.sub(r'[^A-Za-z0-9_.-]', '_', run_id)}"
        env, keyfile = dict(os.environ), None
        if params["dbt_target"] == "bigquery":
            env, keyfile = _bigquery_login(env, target_path)
        try:
            proc = subprocess.run(
                [DBT_BIN, "build", "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR,
                 "--target", params["dbt_target"], "--target-path", target_path,
                 "--vars", json.dumps({"run_date": run_date})],
                capture_output=True, text=True, env=env)
        finally:
            if keyfile and os.path.exists(keyfile):
                os.remove(keyfile)
        print(proc.stdout[-20000:])
        if proc.stderr:
            print(proc.stderr[-5000:])

        put_file, put_string, where = _artifacts(params["dbt_target"])
        prefix = f"orders_daily/{run_id}"
        results = f"{target_path}/run_results.json"
        if os.path.exists(results):
            put_file(results, f"{prefix}/run_results.json")
            print(f"Uploaded {where}/{prefix}/run_results.json")
        else:
            print(f"dbt wrote no run_results.json ({results})")
        put_string(json.dumps({"git_sha": DEPLOYED_COMMIT or None, "dbt_target": params["dbt_target"],
                               "dbt_exit_code": proc.returncode}), f"{prefix}/build.json")

        if proc.returncode != 0:
            raise RuntimeError(f"dbt build failed (exit code {proc.returncode})")
        if not os.path.exists(results):
            raise FileNotFoundError(f"dbt wrote no run_results.json ({results})")
        return f"{prefix}/run_results.json"

    key = extract_orders_to_s3()
    check_raw_with_gx(key) >> load_raw_orders(key) >> dbt_build()


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


def _bigquery_login(env: dict, target_path: str):
    """dbt's BigQuery login, from the Airflow connection lab_gcp like every
    other credential: its key written to a private file for this one dbt
    run (removed after), and the project."""
    from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

    hook = GoogleBaseHook(gcp_conn_id="lab_gcp")
    os.makedirs(target_path, exist_ok=True)
    keyfile = f"{target_path}/.bq-key.json"
    fd = os.open(keyfile, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        key = hook._get_field("keyfile_dict")
        f.write(key if isinstance(key, str) else json.dumps(key))
    return {**env, "DBT_BQ_KEYFILE": keyfile, "DBT_BQ_PROJECT": hook.project_id}, keyfile


def _artifacts(target: str):
    """Where dbt's results go: S3 (dbt-artifacts), or GCS for "bigquery"."""
    if target == "bigquery":
        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        gcs, bucket = GCSHook(gcp_conn_id="lab_gcp"), Variable.get("orders_gcs_artifacts_bucket")
        return (lambda path, name: gcs.upload(bucket, name, filename=path, mime_type="application/json"),
                lambda text, name: gcs.upload(bucket, name, data=text, mime_type="application/json"),
                f"gs://{bucket}")
    s3 = S3Hook(aws_conn_id="lab_s3")
    if not s3.check_for_bucket(ARTIFACTS_BUCKET):
        s3.create_bucket(bucket_name=ARTIFACTS_BUCKET)
    return (lambda path, name: s3.load_file(path, key=name, bucket_name=ARTIFACTS_BUCKET, replace=True),
            lambda text, name: s3.load_string(text, key=name, bucket_name=ARTIFACTS_BUCKET, replace=True),
            f"s3://{ARTIFACTS_BUCKET}")


def _load_bigquery(data: str, key: str) -> int:
    """The run's day into BigQuery orders_lab.raw_orders (T2, D7-49).

    The file goes to the GCS raw bucket under orders/dt=<run date>/, is
    loaded into a scratch table, and replaces that day's slice (dt) in one
    transaction, stamped with _loaded_at. Earlier days stay. Imported here
    so an Airflow without the Google provider still loads the DAG.
    """
    from google.cloud import bigquery
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
    from airflow.providers.google.cloud.hooks.gcs import GCSHook

    ctx = get_current_context()
    # the business day: the nightly run loads yesterday's orders, so they land in yesterday's slice
    run_date = (ctx["dag_run"].run_after - timedelta(days=1)).strftime("%Y-%m-%d")
    safe_run_id = re.sub(r"[^A-Za-z0-9_]", "_", ctx["run_id"])
    bucket = Variable.get("orders_gcs_raw_bucket")
    name = f"orders/dt={run_date}/{key.rsplit('/', 1)[-1]}"
    GCSHook(gcp_conn_id="lab_gcp").upload(bucket, name, data=data, mime_type="text/csv")
    uri = f"gs://{bucket}/{name}"
    print(f"Copied the file to {uri}")

    hook = BigQueryHook(gcp_conn_id="lab_gcp", location=BQ_LOCATION, use_legacy_sql=False)
    client = hook.get_client(project_id=hook.project_id, location=BQ_LOCATION)
    ds = f"{hook.project_id}.{BQ_DATASET}"
    fields = [bigquery.SchemaField("id", "INT64"), bigquery.SchemaField("customer_id", "INT64"),
              bigquery.SchemaField("status", "STRING"), bigquery.SchemaField("amount", "NUMERIC"),
              bigquery.SchemaField("payment_id", "STRING"), bigquery.SchemaField("created_at", "TIMESTAMP"),
              bigquery.SchemaField("updated_at", "TIMESTAMP")]
    table = bigquery.Table(f"{ds}.raw_orders", schema=fields + [
        bigquery.SchemaField("_source_key", "STRING"), bigquery.SchemaField("_loaded_at", "TIMESTAMP"),
        bigquery.SchemaField("dt", "DATE")])
    table.time_partitioning = bigquery.TimePartitioning(field="dt")
    client.create_table(table, exists_ok=True)

    scratch = f"{ds}._load_raw_orders_{safe_run_id}"
    client.load_table_from_uri(uri, scratch, job_config=bigquery.LoadJobConfig(
        schema=fields, source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE")).result()
    cols = ", ".join(COLUMNS)
    try:
        client.query(f"""
            BEGIN TRANSACTION;
            DELETE FROM `{ds}.raw_orders` WHERE dt = @day;
            INSERT INTO `{ds}.raw_orders` ({cols}, _source_key, _loaded_at, dt)
            SELECT {cols}, @uri, CURRENT_TIMESTAMP(), @day FROM `{scratch}`;
            COMMIT TRANSACTION;""", job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("day", "DATE", run_date),
                bigquery.ScalarQueryParameter("uri", "STRING", uri)])).result()
    finally:
        client.delete_table(scratch, not_found_ok=True)
    loaded = list(client.query(f"SELECT count(*) FROM `{ds}.raw_orders` WHERE dt = @day",
                               job_config=bigquery.QueryJobConfig(query_parameters=[
                                   bigquery.ScalarQueryParameter("day", "DATE", run_date)])).result())[0][0]
    print(f"Loaded {loaded} rows into BigQuery {BQ_DATASET}.raw_orders (dt = {run_date})")
    return loaded


orders_daily()
