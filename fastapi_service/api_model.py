from pathlib import Path
import os
import json
import tempfile
import pandas as pd
import joblib

from fastapi import FastAPI
from google.cloud import storage

app = FastAPI(title="Taxi Trips Model API")

model = None
model_version = None


def parse_gcs_uri(gcs_uri: str):
    if not gcs_uri.startswith("gs://"):
        raise ValueError(f"URI inválida de GCS: {gcs_uri}")

    path = gcs_uri.replace("gs://", "")
    bucket_name, blob_path = path.split("/", 1)

    return bucket_name, blob_path


def download_gcs_file(gcs_uri: str, local_path: str):
    bucket_name, blob_path = parse_gcs_uri(gcs_uri)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    blob.download_to_filename(local_path)

    print(f"Arquivo baixado do GCS: {gcs_uri} -> {local_path}")


def load_latest_model_from_gcs():
    global model, model_version

    latest_json_uri = os.getenv(
        "LATEST_JSON_URI",
        "gs://terraform-demo-bucket-ricardo-001/models/taxi_trips_model/latest.json",
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        latest_local_path = os.path.join(tmpdir, "latest.json")
        model_local_path = os.path.join(tmpdir, "model.pkl")

        print(f"Baixando latest.json de: {latest_json_uri}")
        download_gcs_file(latest_json_uri, latest_local_path)

        with open(latest_local_path, "r", encoding="utf-8") as f:
            latest_data = json.load(f)

        print(f"Conteúdo latest.json: {latest_data}")

        model_gcs_path = latest_data.get("gcs_model_path")
        model_version = latest_data.get("latest_version")

        if not model_gcs_path:
            raise Exception("latest.json não contém gcs_model_path")

        print(f"Baixando modelo versão {model_version}: {model_gcs_path}")
        download_gcs_file(model_gcs_path, model_local_path)

        loaded = joblib.load(model_local_path)

        print(f"Tipo carregado: {type(loaded)}")

        if isinstance(loaded, dict):
            if "model" not in loaded:
                raise Exception("O arquivo model.pkl é um dict, mas não contém a chave 'model'")
            model = loaded["model"]
        else:
            model = loaded

        print(f"Tipo final do modelo: {type(model)}")

        if not hasattr(model, "predict"):
            raise Exception("Objeto final NÃO é um modelo válido")

        print(f"Modelo carregado com sucesso. Versão: {model_version}")


@app.on_event("startup")
def startup_event():
    load_latest_model_from_gcs()


@app.get("/")
def root():
    return {
        "status": "API rodando",
        "model_version": model_version,
    }


@app.post("/predict")
def predict(data: dict):
    global model

    if model is None:
        return {"error": "Modelo não carregado"}

    try:
        features = pd.DataFrame([{
            "day_of_week": data["day_of_week"],
            "is_weekend": data["is_weekend"],
            "month": data["month"],
            "day": data["day"],
            "total_passengers": 10000,
            "avg_trip_distance": 3.5,
            "avg_fare_amount": 15.0,
            "lag_1_total_trips": 30000,
            "lag_7_total_trips": 28000,
            "rolling_avg_7_days": 29000,
            "rolling_avg_14_days": 28500,
        }])

        print(f"Features recebidas: {features}")

        prediction = float(model.predict(features)[0])

        return {
            "prediction": round(prediction, 2),
            "model_version": model_version,
        }

    except Exception as e:
        print(f"ERRO NO PREDICT: {e}")
        return {
            "error": str(e)
        }