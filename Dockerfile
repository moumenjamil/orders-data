FROM astrocrpublic.azurecr.io/runtime:3.3-9

# dbt in its own virtualenv: dbt's and Airflow's dependency pins conflict
# (same as the local lab image). The venv uses Python 3.12 because dbt 1.10
# does not run on the image's Python 3.14; uv fetches that Python at build time.
ENV UV_PYTHON_INSTALL_DIR=/usr/local/airflow/.uv-python
RUN python -m venv /tmp/uv-tool \
 && /tmp/uv-tool/bin/pip install --no-cache-dir uv \
 && /tmp/uv-tool/bin/uv venv --python 3.12 /usr/local/airflow/dbt-venv \
 && /tmp/uv-tool/bin/uv pip install --no-cache --python /usr/local/airflow/dbt-venv/bin/python dbt-core==1.10.15 dbt-postgres==1.11.0 \
 && rm -rf /tmp/uv-tool
ENV ARGUS_LAB_DBT_BIN=/usr/local/airflow/dbt-venv/bin/dbt \
    ARGUS_LAB_DBT_PROJECT_DIR=/usr/local/airflow/include/dbt \
    DBT_LOG_PATH=/tmp/dbt-logs
# runtime bump
