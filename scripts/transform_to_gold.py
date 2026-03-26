from pathlib import Path
import pandas as pd


def main():
    input_path = Path("data/yellow_tripdata_2021-01.parquet")
    output_dir = Path("data/gold")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "daily_trip_metrics_2021-01.parquet"

    print(f"Lendo arquivo silver: {input_path}")
    df = pd.read_parquet(input_path)

    # Garantir datetime
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["pickup_date"] = df["tpep_pickup_datetime"].dt.date

    # Agregação gold
    gold_df = (
        df.groupby("pickup_date")
        .agg(
            total_trips=("VendorID", "count"),
            total_passengers=("passenger_count", "sum"),
            avg_trip_distance=("trip_distance", "mean"),
            avg_fare_amount=("fare_amount", "mean"),
            total_revenue=("total_amount", "sum"),
        )
        .reset_index()
    )

    print("Prévia da camada gold:")
    print(gold_df.head())

    gold_df.to_parquet(output_path, index=False)
    print(f"Arquivo gold salvo em: {output_path}")


if __name__ == "__main__":
    main()