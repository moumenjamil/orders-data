"""Glue job orders-daily-curate: raw orders files -> the daily orders table.

The AWS version of the dbt model marts/orders_daily: reads one day's raw
orders snapshot (CSV with a header line in dt=YYYY-MM-DD/; the pipeline
writes a full snapshot of the shop's orders each day), aggregates per order
day, writes Parquet to S3 and keeps the table
orders_lab.orders_daily in the catalog, so Athena (and Assertix) can read it.

The files are read as CSV with a header and a fixed schema, not through the
catalog table orders_lab.raw_orders: Spark ignores that table's
skip.header.line.count (Athena honours it), so each file's header line came
through as an order without a date (found 18 Oct, D7-45).

Job arguments (defaults set on the job; a run may override them):
  --database       the catalog database            (orders_lab)
  --source_path    the raw orders files             (s3://<raw bucket>/orders/)
  --target_table   the daily table                  (orders_daily)
  --target_path    where its Parquet files go       (s3://<raw bucket>/curated/orders_daily/)
  --run_date       the snapshot day (optional; default: the newest dt= folder)
Runs started by Airflow or Assertix also carry --airflow_run_id /
--assertix_run_id (only recorded).
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (DecimalType, LongType, StringType, StructField, StructType,
                               TimestampType)

RAW_SCHEMA = StructType([
    StructField("id", LongType()), StructField("customer_id", LongType()),
    StructField("status", StringType()), StructField("amount", DecimalType(12, 2)),
    StructField("payment_id", StringType()),
    StructField("created_at", TimestampType()), StructField("updated_at", TimestampType()),
])

args = getResolvedOptions(sys.argv, ["JOB_NAME", "database", "source_path", "target_table", "target_path"])
run_date = getResolvedOptions(sys.argv, ["run_date"])["run_date"] if "--run_date" in sys.argv else None
sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
job = Job(glue)
job.init(args["JOB_NAME"], args)

db, src, dst, path = args["database"], args["source_path"].rstrip("/"), args["target_table"], args["target_path"]
if run_date is None:
    days = (spark.read.option("header", True).schema(RAW_SCHEMA).option("basePath", f"{src}/")
            .csv(f"{src}/dt=*/").select("dt").distinct().collect())
    run_date = str(max(r["dt"] for r in days))
orders = (spark.read.option("header", True).option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
          .option("mode", "FAILFAST").schema(RAW_SCHEMA).csv(f"{src}/dt={run_date}/"))

paid = F.col("status").isin("PAID", "SHIPPED")
cancelled = F.col("status") == "CANCELLED"
money = "decimal(22,2)"
daily = (
    orders.withColumn("order_date", F.to_date("created_at"))
    .groupBy("order_date")
    .agg(
        F.count(F.lit(1)).cast("bigint").alias("order_count"),
        F.sum(F.when(paid, 1).otherwise(0)).cast("bigint").alias("paid_order_count"),
        F.sum("amount").cast(money).alias("total_amount"),
        F.coalesce(F.sum(F.when(paid, F.col("amount"))), F.lit(0)).cast(money).alias("paid_amount"),
        F.sum(F.when(cancelled, 1).otherwise(0)).cast("bigint").alias("cancelled_order_count"),
        F.coalesce(F.sum(F.when(cancelled, F.col("amount"))), F.lit(0)).cast(money).alias("cancelled_amount"),
    )
)

daily.repartition(1).write.mode("overwrite").parquet(path)
spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{dst} (
        order_date date, order_count bigint, paid_order_count bigint,
        total_amount {money}, paid_amount {money},
        cancelled_order_count bigint, cancelled_amount {money})
    STORED AS PARQUET
    LOCATION '{path}'
""")
print(f"orders-daily-curate: snapshot dt={run_date}, {daily.count()} order day(s) from {src} written to {db}.{dst} at {path}")
job.commit()
