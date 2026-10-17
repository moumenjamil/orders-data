# orders-data

The data team's repository in the Argus cloud lab: Airflow DAGs and the
dbt project behind the orders data, deployed to Astronomer (Astro).

- `dags/orders_daily.py` — source Postgres -> S3 -> warehouse `raw`, then
  `dbt build` into the target schema (dev / ci / prod, or `snowflake`:
  the same models in Snowflake `ORDERS_DB.CI`, raw in `ORDERS_DB.RAW`)
  and dbt's results to `s3://dbt-artifacts/orders_daily/<run id>/`.
- `dags/customers_daily.py` — an upstream pipeline orders_daily depends on.
- `include/dbt/` — the dbt project (staging + marts, tests).
- `include/gx/` — the team's Great Expectations rules for the raw orders file,
  checked before it is loaded (a broken rule stops the run).

Every push to `main` and every PR commit is deployed by
`.github/workflows/deploy.yml`, which also tells GitHub the commit is live
(deployment environment `astro-ci`). Argus waits for that, then tests the
data.

Generated from `ai-qa-system/data-lab` by `data-lab/cloud/make-orders-data-repo.sh`;
connections and secrets live in the Astro Deployment, never in this repo.

Cancelled orders: see ORD-1.
Daily report columns are described in schema.yml.
