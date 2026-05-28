import json
import os
from datetime import datetime, timezone
from pathlib import Path

import joblib
import mlflow
import mlflow.sklearn
import pandas as pd
from google.cloud import bigquery
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split


# =========================
# CONFIG
# =========================

PROJECT_ID = "terraform-lab-ricardo"
DATASET_ID = "ny_taxi"
TABLE_ID = "features_daily_trip_metrics"

MODEL_NAME = "taxi_trips_model"
MLFLOW_EXPERIMENT_NAME = "taxi_trips_forecasting"

BASE_DIR = Path(__file__).resolve().parents[1]
MODELS_DIR = BASE_DIR / "models" / MODEL_NAME

VERSION = datetime.now(timezone.utc).strftime("v%Y%m%d_%H%M%S")
MODEL_VERSION_DIR = MODELS_DIR / VERSION

MODEL_PATH = MODEL_VERSION_DIR / "model.pkl"
METADATA_PATH = MODEL_VERSION_DIR / "metadata.json"

FEATURE_COLUMNS = [
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

TARGET_COLUMN = "total_trips"


# =========================
# LOAD DATA
# =========================

def load_data() -> pd.DataFrame:
    client = bigquery.Client(project=PROJECT_ID)

    query = f"""
        SELECT
            pickup_date,
            {", ".join(FEATURE_COLUMNS)},
            {TARGET_COLUMN}
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE {TARGET_COLUMN} IS NOT NULL
          AND lag_1_total_trips IS NOT NULL
          AND lag_7_total_trips IS NOT NULL
          AND rolling_avg_7_days IS NOT NULL
          AND rolling_avg_14_days IS NOT NULL
        ORDER BY pickup_date
    """

    df = client.query(query).to_dataframe()
    return df


# =========================
# TRAIN
# =========================

def train_model(df: pd.DataFrame):
    X = df[FEATURE_COLUMNS]
    y = df[TARGET_COLUMN]

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        shuffle=False,
    )

    model = RandomForestRegressor(
        n_estimators=200,
        max_depth=10,
        random_state=42,
        n_jobs=-1,
    )

    model.fit(X_train, y_train)

    predictions = model.predict(X_test)

    mae = mean_absolute_error(y_test, predictions)
    target_mean = y_test.mean()
    mae_percent = (mae / target_mean) * 100

    return model, mae, mae_percent, target_mean, X_train, X_test, y_train, y_test


# =========================
# SAVE LOCAL ARTIFACTS
# =========================

def save_artifacts(model, metadata: dict):
    MODEL_VERSION_DIR.mkdir(parents=True, exist_ok=True)

    joblib.dump(model, MODEL_PATH)

    with open(METADATA_PATH, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=4, ensure_ascii=False)

    print(f"Modelo salvo em: {MODEL_PATH}")
    print(f"Metadata salva em: {METADATA_PATH}")


# =========================
# MAIN
# =========================

def main():
    print("Carregando dados do BigQuery...")
    df = load_data()

    print("Dados carregados:")
    print(df.head())
    print(f"Total de linhas: {len(df)}")

    if df.empty:
        raise ValueError("Nenhum dado encontrado para treinamento.")

    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)

    with mlflow.start_run(run_name=VERSION):
        print("Treinando modelo...")

        model, mae, mae_percent, target_mean, X_train, X_test, y_train, y_test = train_model(df)

        metadata = {
            "model_name": MODEL_NAME,
            "version": VERSION,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "project_id": PROJECT_ID,
            "dataset_id": DATASET_ID,
            "table_id": TABLE_ID,
            "target_column": TARGET_COLUMN,
            "feature_columns": FEATURE_COLUMNS,
            "algorithm": "RandomForestRegressor",
            "parameters": {
                "n_estimators": 200,
                "max_depth": 10,
                "random_state": 42,
                "n_jobs": -1,
            },
            "metrics": {
                "mae": mae,
                "target_mean": target_mean,
                "mae_percent": mae_percent,
            },
            "train_rows": len(X_train),
            "test_rows": len(X_test),
            "total_rows": len(df),
        }

        save_artifacts(model, metadata)

        # =========================
        # MLFLOW LOGGING
        # =========================

        mlflow.log_param("model_name", MODEL_NAME)
        mlflow.log_param("version", VERSION)
        mlflow.log_param("algorithm", "RandomForestRegressor")
        mlflow.log_param("target_column", TARGET_COLUMN)
        mlflow.log_param("n_estimators", 200)
        mlflow.log_param("max_depth", 10)
        mlflow.log_param("random_state", 42)
        mlflow.log_param("train_rows", len(X_train))
        mlflow.log_param("test_rows", len(X_test))
        mlflow.log_param("total_rows", len(df))

        mlflow.log_metric("mae", mae)
        mlflow.log_metric("target_mean", target_mean)
        mlflow.log_metric("mae_percent", mae_percent)

        mlflow.log_artifact(str(MODEL_PATH))
        mlflow.log_artifact(str(METADATA_PATH))

        input_example = X_train.head(3)

        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="model",
            registered_model_name=MODEL_NAME,
            input_example=input_example,
        )

        print("")
        print("Treinamento concluído com sucesso.")
        print(f"Versão: {VERSION}")
        print(f"MAE: {mae:.4f}")
        print(f"Média do target: {target_mean:.4f}")
        print(f"MAE percentual: {mae_percent:.4f}%")
        print("")
        print("Registrado no MLflow:")
        print(f"Experimento: {MLFLOW_EXPERIMENT_NAME}")
        print(f"Modelo registrado: {MODEL_NAME}")


if __name__ == "__main__":
    main()