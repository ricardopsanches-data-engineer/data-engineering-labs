from pathlib import Path

import joblib
import numpy as np
from fastapi import FastAPI


app = FastAPI(title="Taxi Trips Model API")

model = None


def load_model():
    global model

    base_path = Path("C:/Users/sanch/data-engineering-lab/models/taxi_trips_model")

    versions = sorted(base_path.glob("v*/model.pkl"))

    if not versions:
        raise Exception("Nenhum model.pkl encontrado na pasta models")

    latest_model_path = versions[-1]

    print(f"Carregando modelo de: {latest_model_path}")

    loaded = joblib.load(latest_model_path)

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


@app.on_event("startup")
def startup_event():
    load_model()


@app.get("/")
def root():
    return {"status": "API rodando"}


@app.post("/predict")
def predict(data: dict):
    global model

    if model is None:
        return {"error": "Modelo não carregado"}

    try:
        features = np.array([[
            data.get("day_of_week", 1),
            data.get("is_weekend", 0),
            data.get("month", 1),
            data.get("day", 1),
            data.get("total_passengers", 10000),
            data.get("avg_trip_distance", 3.5),
            data.get("avg_fare_amount", 15.0),
            data.get("lag_1_total_trips", 30000),
            data.get("lag_7_total_trips", 28000),
            data.get("rolling_avg_7_days", 29000),
            data.get("rolling_avg_14_days", 28500),
        ]], dtype=float)

        print(f"Features recebidas: {features}")

        prediction = model.predict(features)

        return {
            "prediction": float(prediction[0])
        }

    except Exception as e:
        print(f"ERRO NO PREDICT: {e}")
        return {
            "error": str(e)
        }