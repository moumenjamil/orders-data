# orders-data

The data team's repository in the Argus cloud lab: Airflow DAGs and the
dbt project behind the orders data, deployed to Astronomer (Astro).

- `dags/orders_daily.py` — source Postgres -> S3 -> warehouse `raw`, then
  `dbt build` into the target schema (dev / ci / prod) and dbt's results
  to `s3://dbt-artifacts/orders_daily/<run id>/`.
- `dags/customers_daily.py` — an upstream pipeline orders_daily depends on.
- `include/dbt/` — the dbt project (staging + marts, tests).

Every push to `main` and every PR commit is deployed by
`.github/workflows/deploy.yml`, which also tells GitHub the commit is live
(deployment environment `astro-ci`). Argus waits for that, then tests the
data.

Generated from `ai-qa-system/data-lab` by `data-lab/cloud/make-orders-data-repo.sh`;
connections and secrets live in the Astro Deployment, never in this repo.

Tests for this pipeline run automatically on every pull request.
See the data rules in Confluence (space DATA).
