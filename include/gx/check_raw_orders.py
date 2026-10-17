"""The data team's rules for the raw orders file (Great Expectations).

Run by the orders_daily DAG between extract and load, in GX's own
virtualenv:  check_raw_orders.py <orders.csv> <result.json>
Writes GX's validation result (JSON) and exits 1 when a rule broke, so
the load step never runs on bad data. Rules live here, in the repo, and
are reviewed in PRs like any other code.
"""
import json
import sys

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd

RULES = [
    gxe.ExpectColumnValuesToNotBeNull(column="id"),
    gxe.ExpectColumnValuesToBeUnique(column="id"),
    gxe.ExpectColumnValuesToNotBeNull(column="customer_id"),
    gxe.ExpectColumnValuesToBeInSet(column="status", value_set=["CREATED", "PAID", "SHIPPED", "CANCELLED"]),
    gxe.ExpectColumnValuesToBeBetween(column="amount", min_value=0, strict_min=True),
    # A paid order must have a payment (the same rule as dbt's paid_orders_have_payment).
    gxe.ExpectColumnValuesToNotBeNull(column="payment_id", row_condition='status.isin(["PAID", "SHIPPED"])',
                                      condition_parser="pandas"),
]


def main(csv_path, out_path):
    df = pd.read_csv(csv_path)
    ctx = gx.get_context(mode="ephemeral")
    batch = (ctx.data_sources.add_pandas("raw").add_dataframe_asset("raw_orders")
             .add_batch_definition_whole_dataframe("file").get_batch(batch_parameters={"dataframe": df}))
    suite = ctx.suites.add(gx.ExpectationSuite(name="raw_orders"))
    for rule in RULES:
        suite.add_expectation(rule)
    result = batch.validate(suite)
    with open(out_path, "w") as f:
        json.dump(result.to_json_dict(), f)
    s = result.statistics
    print(f"GX raw_orders: {s['successful_expectations']} of {s['evaluated_expectations']} expectations held")
    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))
