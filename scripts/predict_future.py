import os
import pandas as pd
from google.cloud import bigquery
import joblib

# =========================
# CONFIGURAÇÕES
# =========================
PROJECT_ID = "terraform-lab-ricardo"
DATASET_ID = "ny_taxi"

FEATURES_TABLE = f"{PROJECT_ID}.{DATASET_ID}.features_daily_trip_metrics"
FORECAST_TABLE = f"{PROJECT_ID}.{DATASET_ID}.trips_forecast"

MODEL_PATH = "../models/taxi_trips_model.pkl"

DATE_COL = "pickup_date"
TARGET_COL = "total_trips"

FORECAST_DAYS = 7


# =========================
# CARREGAR MODELO
# =========================
def load_model_artifact():
    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError(f"Modelo não encontrado em: {MODEL_PATH}")

    artifact = joblib.load(MODEL_PATH)

    model = artifact["model"]
    features = artifact["features"]

    print(f"Modelo carregado de: {MODEL_PATH}")
    print(f"Features do modelo: {features}")

    return model, features


# =========================
# CARREGAR HISTÓRICO DO BIGQUERY
# =========================
def load_features_from_bq(client: bigquery.Client, features: list) -> pd.DataFrame:
    extra_cols = ["total_trips"]

    selected_cols = [DATE_COL] + features + extra_cols
    selected_cols = list(dict.fromkeys(selected_cols))  # remove duplicadas mantendo ordem

    cols = ",\n            ".join(selected_cols)

    query = f"""
        SELECT
            {cols}
        FROM `{FEATURES_TABLE}`
        WHERE {DATE_COL} IS NOT NULL
          AND total_trips IS NOT NULL
        ORDER BY {DATE_COL}
    """

    df = client.query(query).to_dataframe()
    df[DATE_COL] = pd.to_datetime(df[DATE_COL])

    return df

# =========================
# PREPARAR HISTÓRICO
# =========================
def prepare_history(df: pd.DataFrame, features: list) -> pd.DataFrame:
    required_cols = [DATE_COL, "total_trips"] + features

    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Colunas ausentes na tabela de features: {missing_cols}")

    df = df.dropna(subset=required_cols)
    df = df.sort_values(DATE_COL).reset_index(drop=True)

    if len(df) < 14:
        raise ValueError("Histórico insuficiente para previsão. Mínimo necessário: 14 linhas válidas.")

    return df


# =========================
# MONTAR FEATURES FUTURAS
# =========================
def build_next_feature_row(history: pd.DataFrame, next_date: pd.Timestamp, features: list) -> pd.DataFrame:
    history = history.sort_values(DATE_COL).reset_index(drop=True)

    last_1 = history.iloc[-1]
    last_7 = history.iloc[-7]
    last_7_window = history.tail(7)
    last_14_window = history.tail(14)

    row = {
        "day_of_week": next_date.dayofweek + 1,
        "is_weekend": 1 if next_date.dayofweek >= 5 else 0,
        "month": next_date.month,
        "day": next_date.day,
        "total_passengers": float(last_7_window["total_passengers"].mean()),
        "avg_trip_distance": float(last_7_window["avg_trip_distance"].mean()),
        "avg_fare_amount": float(last_7_window["avg_fare_amount"].mean()),
        "lag_1_total_trips": float(last_1["total_trips"]),
        "lag_7_total_trips": float(last_7["total_trips"]),
        "rolling_avg_7_days": float(last_7_window["total_trips"].mean()),
        "rolling_avg_14_days": float(last_14_window["total_trips"].mean()),
    }

    return pd.DataFrame([row])[features]


# =========================
# GERAR PREVISÃO
# =========================
def forecast_future(model, history: pd.DataFrame, features: list, forecast_days: int) -> pd.DataFrame:
    history = history.copy().sort_values(DATE_COL).reset_index(drop=True)
    forecasts = []

    last_date = history[DATE_COL].max()

    for _ in range(forecast_days):
        next_date = last_date + pd.Timedelta(days=1)

        x_next = build_next_feature_row(history, next_date, features)
        predicted_total_trips = float(model.predict(x_next)[0])

        forecast_row = {
            "forecast_date": next_date.date(),
            "predicted_total_trips": predicted_total_trips,
            "model": "RandomForestRegressor",
            "created_at": pd.Timestamp.utcnow(),
        }

        forecasts.append(forecast_row)

        synthetic_history_row = {
            DATE_COL: next_date,
            "day_of_week": int(x_next.iloc[0]["day_of_week"]),
            "is_weekend": int(x_next.iloc[0]["is_weekend"]),
            "month": int(x_next.iloc[0]["month"]),
            "day": int(x_next.iloc[0]["day"]),
            "total_passengers": float(x_next.iloc[0]["total_passengers"]),
            "avg_trip_distance": float(x_next.iloc[0]["avg_trip_distance"]),
            "avg_fare_amount": float(x_next.iloc[0]["avg_fare_amount"]),
            "lag_1_total_trips": float(x_next.iloc[0]["lag_1_total_trips"]),
            "lag_7_total_trips": float(x_next.iloc[0]["lag_7_total_trips"]),
            "rolling_avg_7_days": float(x_next.iloc[0]["rolling_avg_7_days"]),
            "rolling_avg_14_days": float(x_next.iloc[0]["rolling_avg_14_days"]),
            "total_trips": predicted_total_trips,
        }

        history = pd.concat([history, pd.DataFrame([synthetic_history_row])], ignore_index=True)
        last_date = next_date

    return pd.DataFrame(forecasts)


# =========================
# SALVAR NO BIGQUERY
# =========================
def save_forecast_to_bq(client: bigquery.Client, forecast_df: pd.DataFrame) -> None:
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    job = client.load_table_from_dataframe(
        forecast_df,
        FORECAST_TABLE,
        job_config=job_config,
    )

    job.result()


# =========================
# MAIN
# =========================
def main() -> None:
    print("Iniciando previsão futura...")

    client = bigquery.Client(project=PROJECT_ID)

    model, features = load_model_artifact()

    print("Lendo features do BigQuery...")
    df = load_features_from_bq(client, features)

    print("Preparando histórico...")
    history = prepare_history(df, features)

    print(f"Linhas válidas no histórico: {len(history)}")
    print(f"Período: {history[DATE_COL].min().date()} até {history[DATE_COL].max().date()}")

    print(f"Gerando previsão para os próximos {FORECAST_DAYS} dias...")
    forecast_df = forecast_future(model, history, features, FORECAST_DAYS)

    print("Previsões geradas:")
    print(forecast_df)

    print(f"Salvando resultado em {FORECAST_TABLE}...")
    save_forecast_to_bq(client, forecast_df)

    print("Processo concluído com sucesso.")


if __name__ == "__main__":
    main()