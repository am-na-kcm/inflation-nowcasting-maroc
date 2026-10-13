# Databricks notebook source
# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import functions as F

CATALOG = "workspace"


# 1. Fonction upsert
def upsert(df, table, keys):
    if not spark.catalog.tableExists(table):
        df.write.format("delta").saveAsTable(table)
        return
    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (
        DeltaTable.forName(spark, table)
        .alias("t")
        .merge(df.alias("s"), cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


# 2. Lecture depuis bronze.calendar
bronze_cal = spark.table(f"{CATALOG}.bronze.calendar")

# 3. Typage et nettoyage
silver_cal = (
    bronze_cal.select(
        F.to_date("month").alias("month"),
        F.col("ramadan_share").cast("double").alias("ramadan_share"),
        F.col("eid_adha_month").cast("int").alias("eid_adha_month"),
    )
    .filter("month is not null")
    .dropDuplicates(["month"])
)

# 4. Écriture dans workspace.silver.calendar
upsert(silver_cal, f"{CATALOG}.silver.calendar", ["month"])

print("Table workspace.silver.calendar créée avec succès !")
