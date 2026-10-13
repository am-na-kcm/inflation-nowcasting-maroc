# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./silver_utils

# COMMAND ----------

display(spark.table("workspace.bronze.hcp_cpi"))

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType

CATALOG = "workspace"

# 1. Lecture de la table Bronze
raw_pdf = spark.table(f"{CATALOG}.bronze.hcp_cpi").toPandas()

# 2. Localisation de l'en-tête et des données Nationales
header_row = raw_pdf[raw_pdf["_c0"] == "Villes"].iloc[0]
nat_pdf = raw_pdf[raw_pdf["_c0"] == "National"]

# 3. Correspondances des séries et des mois
series_mapping = {
    "(GR) GENERAL": "global",
    "(01) PRODUITS ALIMENTAIRES ET BOISSONS NON ALCOOLISÉES": "alimentation",
    "(07) TRANSPORTS": "transport",
}

month_mapping = {
    "janv": "01",
    "févr": "02",
    "mars": "03",
    "avr": "04",
    "mai": "05",
    "juin": "06",
    "juill": "07",
    "août": "08",
    "sept": "09",
    "oct": "10",
    "nov": "11",
    "déc": "12",
}

# 4. Dépivotage et standardisation
parsed_records = []

for raw_label, target_series in series_mapping.items():
    subset = nat_pdf[nat_pdf["_c1"] == raw_label]
    if subset.empty:
        continue
    row = subset.iloc[0]

    for col in raw_pdf.columns:
        if col in ["_c0", "_c1", "_rescued_data", "_ingested_at", "_source", "_file"]:
            continue

        col_label = header_row[col]
        if pd.notna(col_label) and "-" in str(col_label):
            parts = str(col_label).split("-")
            if len(parts) == 2:
                m_str, y_str = parts[0].strip().lower(), parts[1].strip()
                m_num = month_mapping.get(m_str)
                if m_num and y_str.isdigit():
                    date_val = pd.to_datetime(f"{y_str}-{m_num}-01").date()
                    val_str = row[col]
                    if pd.notna(val_str):
                        try:
                            cpi_val = float(str(val_str).replace(",", "."))
                            parsed_records.append(
                                {"month": date_val, "series": target_series, "cpi_index": cpi_val}
                            )
                        except ValueError:
                            pass

# 5. Conversion vers DataFrame Spark typé
schema = StructType(
    [
        StructField("month", DateType(), False),
        StructField("series", StringType(), False),
        StructField("cpi_index", DoubleType(), False),
    ]
)

cpi_spark_df = spark.createDataFrame(pd.DataFrame(parsed_records), schema=schema)

# 6. Contrôles qualité
check(
    cpi_spark_df,
    "cpi_monthly",
    {
        "month_not_null": "month is not null",
        "series_not_null": "series is not null",
        "cpi_strictly_positive": "cpi_index > 0",
    },
)

# 7. Contrôle de continuité mensuelle (obligatoire selon le guide)
cpi_global = cpi_spark_df.filter("series = 'global'")
bounds = cpi_global.agg(F.min("month").alias("a"), F.max("month").alias("b")).first()

expected = spark.sql(
    f"SELECT sequence(to_date('{bounds.a}'), to_date('{bounds.b}'), interval 1 month) AS m"
).select(F.explode("m").alias("month"))

missing = expected.join(cpi_global, "month", "left_anti").count()
assert missing == 0, f"{missing} mois manquants dans la série CPI !"
print(f"Continuité validée : 0 mois manquant entre {bounds.a} et {bounds.b}.")

# 8. Upsert idempotent dans la table Silver
upsert(cpi_spark_df, f"{CATALOG}.silver.cpi_monthly", ["month", "series"])
