# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./silver_utils

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType

CATALOG = "workspace"

# 1. Récupération du snapshot le plus récent par ville
b = latest(spark.table(f"{CATALOG}.bronze.weather"), ["city"])

# 2. Définition du schéma exact du bloc "daily" visible dans le JSON
daily_schema = StructType(
    [
        StructField("time", ArrayType(StringType()), True),
        StructField("temperature_2m_mean", ArrayType(DoubleType()), True),
        StructField("precipitation_sum", ArrayType(DoubleType()), True),
    ]
)

# 3. Conversion STRING -> STRUCT si le type n'a pas été inféré dans Bronze
daily_col = (
    F.from_json(F.col("daily"), daily_schema)
    if dict(b.dtypes)["daily"] == "string"
    else F.col("daily")
)

# 4. Déballage (arrays_zip) et éclatement (explode) en lignes journalières
daily = (
    b.withColumn("daily_struct", daily_col)
    .select(
        "city",
        F.explode(
            F.arrays_zip(
                "daily_struct.time",
                "daily_struct.temperature_2m_mean",
                "daily_struct.precipitation_sum",
            )
        ).alias("z"),
    )
    .select(
        "city",
        F.to_date("z.time").alias("date"),
        F.col("z.temperature_2m_mean").cast("double").alias("temp_mean"),
        F.col("z.precipitation_sum").cast("double").alias("precip"),
    )
)

# 5. Agrégation mensuelle par ville
by_city = daily.groupBy("city", F.trunc("date", "month").alias("month")).agg(
    F.avg("temp_mean").alias("temp_mean"),
    F.sum("precip").alias("precip_sum"),
    F.countDistinct("date").alias("n_days"),
)

# 6. Agrégation mensuelle nationale
national = by_city.groupBy("month").agg(
    F.avg("temp_mean").alias("temp_mean"),
    F.avg("precip_sum").alias("precip_sum"),
    F.min("n_days").alias("n_days"),
)

# 7. Contrôles qualité bloquants
check(
    national,
    "weather_monthly",
    {
        "month not null": "month is not null",
        "temp plausible": "temp_mean between 10 and 45",
        "precip >= 0": "precip_sum >= 0",
    },
)

# 8. Écriture idempotente (MERGE / upsert)
upsert(national, f"{CATALOG}.silver.weather_monthly", ["month"])
