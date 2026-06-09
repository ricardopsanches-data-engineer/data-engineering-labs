import argparse
from pathlib import Path
import pandas as pd


def main():
    parser = argparse.ArgumentParser(description="Transforma arquivo Silver em Gold")
    parser.add_argument("--year", required=True, help="Ano com 4 dígitos, ex: 2021")
    parser.add_argument("--month", required=True, help="Mês com 2 dígitos, ex: 02")
    args = parser.parse_args()

    year = args.year
    month = args.month

    source_file = f"yellow_tripdata_{year}-{month}.parquet"
    silver_path = Path("/data/silver/trips") / source_file

    gold_dir = Path("/data/gold")
    gold_dir.mkdir(parents=True, exist_ok=True)

    gold_file = f"daily_trip_metrics_{year}-{month}.parquet"
    gold_path = gold_dir / gold_file

    print(f"Lendo Silver: {silver_path}")

    if not silver_path.exists():
        raise FileNotFoundError(f"Arquivo Silver não encontrado: {silver_path}")

    df = pd.read_parquet(silver_path)

    if "pickup_date" in df.columns:
        df["pickup_date"] = pd.to_datetime(df["pickup_date"], errors="coerce")

    numeric_cols = [
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "total_amount",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    gold_df = (
        df.groupby("pickup_date", as_index=False)
        .agg(
            total_trips=("pickup_date", "size"),
            total_passengers=("passenger_count", "sum"),
            avg_trip_distance=("trip_distance", "mean"),
            avg_fare_amount=("fare_amount", "mean"),
            total_revenue=("total_amount", "sum"),
        )
        .sort_values("pickup_date")
    )

    gold_df["pickup_date"] = pd.to_datetime(gold_df["pickup_date"]).dt.date

    print("Prévia da Gold:")
    print(gold_df.head())

    print(f"Salvando Gold em: {gold_path}")
    gold_df.to_parquet(gold_path, index=False)

    print("Gold gerada com sucesso.")


if __name__ == "__main__":
    main()