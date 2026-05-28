import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timezone

PROJECT_ID = "terraform-lab-ricardo"
DATASET = "ny_taxi"

PREDICTIONS_TABLE = "predictions_api"
DRIFT_TABLE = "prediction_drift_monitoring"

client = bigquery.Client(project=PROJECT_ID)

# =========================
# BUSCAR HISTÓRICO
# =========================

query = f"""
SELECT
    predicted_total_trips
FROM `{PROJECT_ID}.{DATASET}.{PREDICTIONS_TABLE}`
ORDER BY prediction_timestamp DESC
LIMIT 10
"""

df = client.query(query).to_dataframe()

if len(df) < 2:
    raise Exception("Poucos dados para análise de drift")

# =========================
# CÁLCULOS
# =========================

current_prediction = df.iloc[0]["predicted_total_trips"]

historical_average = df.iloc[1:]["predicted_total_trips"].mean()

deviation_percent = abs(
    (current_prediction - historical_average)
    / historical_average
) * 100

# =========================
# CLASSIFICAÇÃO
# =========================

if deviation_percent <= 20:
    drift_status = "NORMAL"

elif deviation_percent <= 40:
    drift_status = "WARNING"

else:
    drift_status = "DRIFT_ALERT"

# =========================
# RESULTADO
# =========================

result_df = pd.DataFrame([{
    "drift_timestamp": datetime.now(timezone.utc),
    "current_prediction": current_prediction,
    "historical_average": historical_average,
    "deviation_percent": deviation_percent,
    "drift_status": drift_status
}])

table_id = f"{PROJECT_ID}.{DATASET}.{DRIFT_TABLE}"

job = client.load_table_from_dataframe(result_df, table_id)

job.result()

print(result_df)
print(f"Drift salvo em {table_id}")