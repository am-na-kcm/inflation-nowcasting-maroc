# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./silver_utils

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"

t = spark.table(f"{CATALOG}.bronze.trends").select(
    F.to_date("date").alias("month"),
    "keyword",
    F.col("value").cast("double").alias("value"),
    F.col("draw").cast("int").alias("draw"),
)

monthly = t.groupBy("month", "keyword").agg(
    F.avg("value").alias("value"), F.count("*").alias("n_draws")
)

check(
    monthly,
    "trends_monthly",
    {"value in range": "value between 0 and 100", "month not null": "month is not null"},
)
upsert(monthly, f"{CATALOG}.silver.trends_monthly", ["month", "keyword"])
