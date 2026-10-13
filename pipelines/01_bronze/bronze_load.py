# Databricks notebook source
from pyspark.sql import functions as F

CATALOG = "workspace"
RAW = f"/Volumes/{CATALOG}/landing/raw"

# COMMAND ----------


def ingest(source, fmt, table, **options):
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", fmt)
        .option("cloudFiles.schemaLocation", f"{RAW}/_schemas/{source}")
        .options(**options)
        .load(f"{RAW}/{source}")
    )
    df = (
        reader.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit(source))
        .withColumn("_file", F.col("_metadata.file_path"))
    )
    (
        df.writeStream.option("checkpointLocation", f"{RAW}/_checkpoints/{source}")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{CATALOG}.bronze.{table}")
        .awaitTermination()
    )


# COMMAND ----------

ingest("weather", "json", "weather", multiLine="true")
ingest("brent", "json", "brent", multiLine="true")
ingest("trends", "csv", "trends", header="true")
ingest("calendar", "csv", "calendar", header="true")
ingest("fx", "csv", "fx", header="true")
ingest("hcp", "csv", "hcp_cpi", header="false")

# COMMAND ----------

for t in ["weather", "brent", "trends", "calendar", "fx", "hcp_cpi"]:
    n = spark.table(f"{CATALOG}.bronze.{t}").count()
    print(t, n)
