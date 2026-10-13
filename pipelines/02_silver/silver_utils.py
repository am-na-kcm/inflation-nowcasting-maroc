# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F


def latest(df, keys, order_col="_ingested_at"):
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


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


def check(df, name, rules):
    # rules = {"label": "sql condition that must be true"}
    failed = []
    for label, cond in rules.items():
        n = df.filter(F.expr(f"NOT coalesce({cond}, false)")).count()
        if n > 0:
            failed.append((label, n))
    if failed:
        raise Exception(f"Quality check failed for {name}: {failed}")
    print(f"quality OK: {name}")


# COMMAND ----------
