import time
import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timezone

# =========================
# CONFIG
# =========================

PROJECT_ID = "terraform-lab-ricardo"
DATASET = "ny_taxi"

PREDICTIONS_TABLE = "predictions_api"
LOGS_TABLE = "prediction_api_logs"

API_URL = "http://host.docker.internal:8000/predict"
MODEL_VERSION = "v20260505_110823"

# =========================
# DADOS DE ENTRADA
# =========================

payload = {
    "day_of_week": 2,
    "is_weekend": 0,
    "month": 5,
    "day": 6,
    "total_passengers": 150000,
    "avg_trip_distance": 3.8,
    "avg_fare_amount": 18.5,
    "lag_1_total_trips": 420000,
    "lag_7_total_trips": 415000,
    "rolling_avg_7_days": 418000,
    "rolling_avg_14_days": 416000
}

client = bigquery.Client(project=PROJECT_ID)


def save_log(status, response_time_seconds=None, prediction=None, error_message=None):
    log_df = pd.DataFrame([{
        "log_timestamp": datetime.now(timezone.utc),
        "status": status,
        "api_url": API_URL,
        "response_time_seconds": response_time_seconds,
        "model_version": MODEL_VERSION,
        "prediction": prediction,
        "error_message": error_message
    }])

    table_id = f"{PROJECT_ID}.{DATASET}.{LOGS_TABLE}"
    job = client.load_table_from_dataframe(log_df, table_id)
    job.result()

    print(f"Log salvo em {table_id}")


try:
    start_time = time.time()

    response = requests.post(
        API_URL,
        json=payload,
        timeout=30
    )

    response_time_seconds = time.time() - start_time

    response.raise_for_status()

    prediction_response = response.json()

    print("Resposta da API:")
    print(prediction_response)

    prediction_value = prediction_response["prediction"]

    prediction_df = pd.DataFrame([{
        "prediction_timestamp": datetime.now(timezone.utc),
        "predicted_total_trips": prediction_value,
        "day_of_week": payload["day_of_week"],
        "is_weekend": payload["is_weekend"],
        "month": payload["month"],
        "day": payload["day"]
    }])

    predictions_table_id = f"{PROJECT_ID}.{DATASET}.{PREDICTIONS_TABLE}"

    job = client.load_table_from_dataframe(prediction_df, predictions_table_id)
    job.result()

    print(prediction_df)
    print(f"Previsão salva em {predictions_table_id}")

    save_log(
        status="SUCCESS",
        response_time_seconds=response_time_seconds,
        prediction=prediction_value,
        error_message=None
    )

except Exception as e:
    error_message = str(e)

    print("ERRO NA INFERÊNCIA:")
    print(error_message)

    save_log(
        status="FAILED",
        response_time_seconds=None,
        prediction=None,
        error_message=error_message
    )

    raise