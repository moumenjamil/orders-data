FROM astrocrpublic.azurecr.io/runtime:3.3-9

# dbt in its own virtualenv: dbt's and Airflow's dependency pins conflict
# (same as the local lab image).
RUN python -m venv /usr/local/airflow/dbt-venv \
 && /usr/local/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.10.15 dbt-postgres==1.11.0
ENV ARGUS_LAB_DBT_BIN=/usr/local/airflow/dbt-venv/bin/dbt \
    ARGUS_LAB_DBT_PROJECT_DIR=/usr/local/airflow/include/dbt \
    DBT_LOG_PATH=/tmp/dbt-logs
