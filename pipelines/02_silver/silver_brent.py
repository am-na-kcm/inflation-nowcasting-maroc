# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./silver_utils

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

CATALOG = "workspace"

raw = spark.table(f"{CATALOG}.bronze.brent")

# Définition du schéma et parsing du JSON STRING vers un tableau
obs_schema = ArrayType(
    StructType(
        [
            StructField("date", StringType(), True),
            StructField("value", StringType(), True),
        ]
    )
)
obs_col = (
    F.from_json(F.col("observations"), obs_schema)
    if dict(raw.dtypes)["observations"] == "string"
    else F.col("observations")
)

# Remplacer F.col("o.value").cast("double") par try_cast :
obs = (
    raw.withColumn("obs_parsed", obs_col)
    .select("_ingested_at", F.explode("obs_parsed").alias("o"))
    .select(
        "_ingested_at",
        F.to_date("o.date").alias("date"),
        F.expr("try_cast(o.value AS double)").alias("brent_usd"),
    )
)

obs = latest(obs, ["date"]).filter("brent_usd is not null")

monthly = obs.groupBy(F.trunc("date", "month").alias("month")).agg(
    F.avg("brent_usd").alias("brent_usd")
)

check(
    monthly, "brent_monthly", {"month not null": "month is not null", "price > 0": "brent_usd > 0"}
)
upsert(monthly, f"{CATALOG}.silver.brent_monthly", ["month"])
