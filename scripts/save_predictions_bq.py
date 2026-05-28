import pandas as pd
from google.cloud import bigquery
from sklearn.ensemble import RandomForestRegressor

# =========================
# CONFIG
# =========================
PROJECT_ID = "terraform-lab-ricardo"
DATASET = "ny_taxi"
SOURCE_TABLE = f"{PROJECT_ID}.{DATASET}.features_daily_trip_metrics"
TARGET_TABLE = f"{PROJECT_ID}.{DATASET}.ml_predictions_daily"

# =========================
# BIGQUERY CLIENT
# =========================
client = bigquery.Client(project=PROJECT_ID)

# =========================
# CARREGAR DADOS
# =========================
query = f"""
SELECT
    pickup_date,
    day_of_week,
    is_weekend,
    month,
    day,
    total_passengers,
    avg_trip_distance,
    avg_fare_amount,
    lag_1_total_trips,
    lag_7_total_trips,
    rolling_avg_7_days,
    rolling_avg_14_days,
    total_trips
FROM `{SOURCE_TABLE}`
WHERE lag_1_total_trips IS NOT NULL
  AND lag_7_total_trips IS NOT NULL
  AND rolling_avg_7_days IS NOT NULL
  AND rolling_avg_14_days IS NOT NULL
  AND total_trips > 10000
ORDER BY pickup_date
"""

df = client.query(query).to_dataframe()

print("Dados carregados:")
print(df.head())
print(f"\nTotal de linhas após filtro: {len(df)}")

# =========================
# FEATURES E TARGET
# =========================
features = [
    "day_of_week",
    "is_weekend",
    "month",
    "day",
    "total_passengers",
    "avg_trip_distance",
    "avg_fare_amount",
    "lag_1_total_trips",
    "lag_7_total_trips",
    "rolling_avg_7_days",
    "rolling_avg_14_days",
]

target = "total_trips"

X = df[features]
y = df[target]

# =========================
# SPLIT TEMPORAL
# =========================
train_size = int(len(df) * 0.8)

X_train = X.iloc[:train_size]
X_test = X.iloc[train_size:]
y_train = y.iloc[:train_size]
y_test = y.iloc[train_size:]
dates_test = df["pickup_date"].iloc[train_size:]

# =========================
# TREINAR MODELO
# =========================
model = RandomForestRegressor(
    n_estimators=200,
    max_depth=10,
    random_state=42
)

model.fit(X_train, y_train)
y_pred = model.predict(X_test)

# =========================
# MONTAR RESULTADO
# =========================
results_df = pd.DataFrame({
    "pickup_date": pd.to_datetime(dates_test).dt.date,
    "real_total_trips": y_test.values,
    "pred_total_trips": y_pred,
})

results_df["abs_error"] = (results_df["real_total_trips"] - results_df["pred_total_trips"]).abs()
results_df["model_run_at"] = pd.Timestamp.utcnow()

print("\nPrévia das previsões:")
print(results_df.head())

# =========================
# SALVAR NO BIGQUERY
# =========================
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE"
)

job = client.load_table_from_dataframe(
    results_df,
    TARGET_TABLE,
    job_config=job_config
)
job.result()

print(f"\nPrevisões salvas com sucesso em: {TARGET_TABLE}")
print(f"Total de linhas gravadas: {len(results_df)}")