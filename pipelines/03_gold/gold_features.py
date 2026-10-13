# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = "workspace"
s = lambda t: spark.table(f"{CATALOG}.silver.{t}")

# COMMAND ----------

# target: CPI global, month-on-month and year-on-year change

cpi = s("cpi_monthly").filter("series = 'global'").select("month", "cpi_index")
w = Window.orderBy("month")
cpi = cpi.withColumn(
    "cpi_mom", (F.col("cpi_index") / F.lag("cpi_index", 1).over(w) - 1) * 100
).withColumn("cpi_yoy", (F.col("cpi_index") / F.lag("cpi_index", 12).over(w) - 1) * 100)

# COMMAND ----------

# Google Trends: one column per keyword

tr = s("trends_monthly").groupBy("month").pivot("keyword").avg("value")
for c in tr.columns:
    tr = tr.withColumnRenamed(c, "gt_" + c.replace(" ", "_") if c != "month" else c)

# composite indices (simple averages by theme)
FOOD_KW = ["huile", "tomate", "poulet", "viande", "mouton"]
food = [c for c in tr.columns if any(k in c for k in FOOD_KW)]
fuel = [c for c in tr.columns if any(k in c for k in ["gasoil", "essence"])]
tr = tr.withColumn("gt_food", sum(F.col(c) for c in food) / len(food)).withColumn(
    "gt_fuel", sum(F.col(c) for c in fuel) / len(fuel)
)

# COMMAND ----------

g = (
    s("calendar")
    .select(F.col("month"), "ramadan_share", "eid_adha_month")
    .join(tr, "month", "left")
    .join(s("weather_monthly").select("month", "temp_mean", "precip_sum"), "month", "left")
    .join(s("brent_monthly"), "month", "left")
    .join(cpi, "month", "left")
)

# lags on exogenous features (Brent, weather, trends themes)
w = Window.orderBy("month")
for c in ["gt_food", "gt_fuel", "brent_usd", "precip_sum"]:
    for k in (1, 2, 3):
        g = g.withColumn(f"{c}_lag{k}", F.lag(c, k).over(w))

# CPI lags: lag 1 is the latest value known when nowcasting month M
for k in (1, 2, 3, 12):
    g = g.withColumn(f"cpi_mom_lag{k}", F.lag("cpi_mom", k).over(w))

# calendar: the month before Eid al-Adha drives sheep and meat prices
g = g.withColumn("eid_next_month", F.lead("eid_adha_month", 1).over(w))

g = g.filter("month <= current_date()").orderBy("month")

# COMMAND ----------

# gold is derived data: full rebuild is idempotent

(
    g.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.features_monthly")
)

spark.sql(
    f"COMMENT ON TABLE {CATALOG}.gold.features_monthly IS "
    "'One row per month: CPI target, lagged CPI, Google Trends, Brent, weather, calendar'"
)
