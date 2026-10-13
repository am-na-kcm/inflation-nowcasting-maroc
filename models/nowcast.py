# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# pip install statsmodels


# COMMAND ----------

import mlflow
import numpy as np
import pandas as pd
from sklearn.linear_model import RidgeCV
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from statsmodels.tsa.statespace.sarimax import SARIMAX

CATALOG = "workspace"
df = (
    spark.table(f"{CATALOG}.gold.features_monthly")
    .toPandas()
    .sort_values("month")
    .set_index("month")
)

TARGET = "cpi_mom"
EXOG = ["gt_food_lag0", "gt_fuel_lag0", "brent_usd_lag1", "ramadan_share", "eid_next_month"]
# note: lag0 variables are the month's own values (available at end of month)
df["gt_food_lag0"] = df["gt_food"]
df["gt_fuel_lag0"] = df["gt_fuel"]
RIDGE_COLS = [c for c in df.columns if "lag" in c or c in EXOG]
RIDGE_COLS = [c for c in RIDGE_COLS if not c.startswith("cpi_") or c.startswith("cpi_mom_lag")]

data = df[[TARGET] + sorted(set(EXOG + RIDGE_COLS))].dropna()
y = data[TARGET]
START = 60  # first forecast after 60 months of history


def rmse(a, b):
    return float(np.sqrt(np.mean((np.asarray(a) - np.asarray(b)) ** 2)))


def forecast_sarima(y):
    preds = []
    for t in range(START, len(y)):
        m = SARIMAX(y.iloc[:t], order=(1, 0, 0), seasonal_order=(1, 0, 0, 12)).fit(disp=False)
        preds.append(float(m.forecast(1).iloc[0]))
    return pd.Series(preds, index=y.index[START:])


def forecast_sarimax(y, X):
    preds = []
    for t in range(START, len(y)):
        Xtr = (X.iloc[:t] - X.iloc[:t].mean()) / X.iloc[:t].std()
        xt = (X.iloc[[t]] - X.iloc[:t].mean()) / X.iloc[:t].std()
        m = SARIMAX(y.iloc[:t], exog=Xtr, order=(1, 0, 0), seasonal_order=(1, 0, 0, 12)).fit(
            disp=False
        )
        preds.append(float(m.forecast(1, exog=xt).iloc[0]))
    return pd.Series(preds, index=y.index[START:])


def forecast_ridge(y, X):
    preds = []
    for t in range(START, len(y)):
        model = make_pipeline(StandardScaler(), RidgeCV(alphas=np.logspace(-2, 3, 20)))
        model.fit(X.iloc[:t], y.iloc[:t])
        preds.append(float(model.predict(X.iloc[[t]])[0]))
    return pd.Series(preds, index=y.index[START:])


def diebold_mariano(e1, e2):
    # one-step-ahead, squared error loss; e1 = benchmark errors, e2 = candidate errors
    from scipy import stats

    d = np.asarray(e1) ** 2 - np.asarray(e2) ** 2
    stat = d.mean() / np.sqrt(d.var(ddof=1) / len(d))
    return float(stat), float(2 * (1 - stats.norm.cdf(abs(stat))))


mlflow.set_experiment("/Users/amnaa.benkacem@gmail.com/inflation_nowcast")
actual = y.iloc[START:]
results = {}

for name, fn in {
    "sarima_benchmark": lambda: forecast_sarima(y),
    "sarimax_enriched": lambda: forecast_sarimax(y, data[EXOG]),
    "ridge_all_features": lambda: forecast_ridge(y, data[RIDGE_COLS]),
}.items():
    with mlflow.start_run(run_name=name):
        pred = fn()
        results[name] = pred
        mlflow.log_param("model", name)
        mlflow.log_param("start", START)
        mlflow.log_param("n_obs", len(y))
        mlflow.log_metric("rmse", rmse(actual, pred))
        mlflow.log_metric("mae", float(np.mean(np.abs(actual - pred))))
        if name != "sarima_benchmark":
            bench = results["sarima_benchmark"]
            mlflow.log_metric("rmse_relative", rmse(actual, pred) / rmse(actual, bench))
            stat, p = diebold_mariano(actual - bench, actual - pred)
            mlflow.log_metric("dm_stat", stat)
            mlflow.log_metric("dm_pvalue", p)
        print(name, "RMSE", round(rmse(actual, pred), 4))

# COMMAND ----------

best = "sarimax_enriched"  # choose according to the validation results
out = pd.DataFrame(
    {
        "month": pd.to_datetime(actual.index).date,
        "actual_cpi_mom": actual.values,
        "pred_benchmark": results["sarima_benchmark"].values,
        "pred_model": results[best].values,
    }
)
out["model"] = best
out["run_ts"] = pd.Timestamp.now()
(
    spark.createDataFrame(out)
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.nowcast")
)

# (
#   spark.table("workspace.gold.nowcast")
#  .toPandas()
# .to_csv("/Volumes/workspace/gold/exports/nowcast.csv", index=False)
# )

# COMMAND ----------
