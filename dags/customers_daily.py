"""customers_daily — an upstream pipeline in the Argus data lab.

orders_daily reads what this one produces (in the lab only in the
project configuration: orders_daily lists it in dependencyIds), so
Argus checks this pipeline's last run before it tests orders_daily
(D7-31). Its outcome is chosen per run, so every case can be shown:
{"outcome": "success"|"fail", "seconds": N} (N = how long it runs).
"""
from __future__ import annotations

import time
from datetime import datetime

from airflow.sdk import Param, dag, task


@dag(
    dag_id="customers_daily",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={
        "outcome": Param("success", enum=["success", "fail"]),
        "seconds": Param(5, type="integer", minimum=0, maximum=600),
    },
    tags=["argus-data-lab"],
)
def customers_daily():

    @task
    def refresh_customers(params=None) -> str:
        time.sleep(int(params["seconds"]))
        if params["outcome"] == "fail":
            raise RuntimeError("customers source unreachable (lab: outcome=fail)")
        print("customers refreshed")
        return "ok"

    refresh_customers()


customers_daily()
