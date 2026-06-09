import os
import pandas as pd
from google.cloud import bigquery
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
import joblib

# =========================
# CONFIGURAÇÕES
# =========================
PROJECT_ID = "terraform-lab-ricardo"
FEATURE_TABLE = "terraform-lab-ricardo.ny_taxi.features_daily_trip_metrics"

MODEL_DIR = "../models"
MODEL_PATH = os.path.join(MODEL_DIR, "taxi_trips_model.pkl")

os.makedirs(MODEL_DIR, exist_ok=True)

# =========================
# CARREGAR DADOS DO BIGQUERY
# =========================
client = bigquery.Client(project=PROJECT_ID)

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
FROM `{FEATURE_TABLE}`
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
print(f"\nTotal de linhas após filtro de lags: {len(df)}")

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

print(f"\nLinhas de treino: {len(X_train)}")
print(f"Linhas de teste: {len(X_test)}")

# =========================
# MODELO
# =========================
model = RandomForestRegressor(
    n_estimators=200,
    max_depth=10,
    random_state=42
)

model.fit(X_train, y_train)

# =========================
# PREVISÃO E MÉTRICAS
# =========================
y_pred = model.predict(X_test)

mae = mean_absolute_error(y_test, y_pred)
mae_percent = (mae / y_test.mean()) * 100

print("\nResultado do modelo:")
print(f"MAE: {mae}")
print(f"Média do target no teste: {y_test.mean()}")
print(f"MAE percentual (%): {mae_percent:.2f}")

# =========================
# SALVAR ARTEFATO DO MODELO
# =========================
model_artifact = {
    "model": model,
    "features": features,
    "target": target,
    "mae": mae,
    "mae_percent": mae_percent,
}

joblib.dump(model_artifact, MODEL_PATH)

print(f"\nModelo salvo em: {MODEL_PATH}")

# =========================
# COMPARAÇÃO
# =========================
resultado = X_test.copy()
resultado["real_total_trips"] = y_test.values
resultado["pred_total_trips"] = y_pred

print("\nComparação real vs previsto:")
print(resultado[["real_total_trips", "pred_total_trips"]].head(10))

# =========================
# IMPORTÂNCIA DAS FEATURES
# =========================
importancias = pd.DataFrame({
    "feature": features,
    "importance": model.feature_importances_
}).sort_values(by="importance", ascending=False)

print("\nImportância das features:")
print(importancias)