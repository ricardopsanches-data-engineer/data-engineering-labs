import argparse
from pathlib import Path
import pandas as pd


def main():
    parser = argparse.ArgumentParser(description="Transforma arquivo Bronze em Silver")
    parser.add_argument("--year", required=True, help="Ano com 4 dígitos, ex: 2021")
    parser.add_argument("--month", required=True, help="Mês com 2 dígitos, ex: 02")
    args = parser.parse_args()

    year = args.year
    month = args.month

    file_name = f"yellow_tripdata_{year}-{month}.parquet"

    bronze_path = Path("/data/bronze/trips") / file_name
    silver_dir = Path("/data/silver/trips")
    silver_dir.mkdir(parents=True, exist_ok=True)
    silver_path = silver_dir / file_name

    print(f"Lendo Bronze: {bronze_path}")

    if not bronze_path.exists():
        raise FileNotFoundError(f"Arquivo Bronze não encontrado: {bronze_path}")

    df = pd.read_parquet(bronze_path)

    print("Colunas encontradas:")
    print(df.columns.tolist())

    if "tpep_pickup_datetime" in df.columns:
        df["tpep_pickup_datetime"] = pd.to_datetime(
            df["tpep_pickup_datetime"], errors="coerce"
        )

    if "tpep_dropoff_datetime" in df.columns:
        df["tpep_dropoff_datetime"] = pd.to_datetime(
            df["tpep_dropoff_datetime"], errors="coerce"
        )

    df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime"])

    numeric_cols = [
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "total_amount",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "trip_distance" in df.columns:
        df = df[df["trip_distance"].fillna(0) >= 0]

    if "fare_amount" in df.columns:
        df = df[df["fare_amount"].fillna(0) >= 0]

    if "passenger_count" in df.columns:
        df = df[df["passenger_count"].fillna(0) >= 0]

    df["pickup_date"] = df["tpep_pickup_datetime"].dt.date

    print(f"Quantidade de linhas após limpeza: {len(df)}")
    print(f"Salvando Silver em: {silver_path}")

    df.to_parquet(silver_path, index=False)

    print("Silver gerado com sucesso.")


if __name__ == "__main__":
    main()