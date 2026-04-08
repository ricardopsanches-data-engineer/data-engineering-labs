from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

import pandas as pd
import requests


BUCKET_NAME = "terraform-demo-bucket-ricardo-001"


def month_iterator(start_year: int, start_month: int):
    year = start_year
    month = start_month

    while True:
        yield year, month
        month += 1
        if month > 12:
            month = 1
            year += 1


def gcs_cp(source: str, target: str) -> None:
    subprocess.run(
        ["gcloud", "storage", "cp", source, target],
        check=True,
    )


def gcs_ls(target: str) -> bool:
    result = subprocess.run(
        ["gcloud", "storage", "ls", target],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0 and target in result.stdout


def process_month(year: int, month: int, data_dir: Path) -> None:
    ym = f"{year}-{month:02d}"

    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{ym}.parquet"

    raw_path = data_dir / f"yellow_tripdata_{ym}.parquet"
    silver_path = data_dir / f"silver_{ym}.parquet"
    gold_path = data_dir / f"gold_{ym}.parquet"

    bronze_target = (
        f"gs://{BUCKET_NAME}/bronze/trips/yellow_tripdata_{ym}.parquet"
    )
    silver_target = (
        f"gs://{BUCKET_NAME}/silver/trips/yellow_tripdata_{ym}.parquet"
    )
    gold_target = (
        f"gs://{BUCKET_NAME}/gold/daily_trip_metrics_{ym}.parquet"
    )

    print(f"\n=== Processando {ym} ===")
    print(f"Baixando da origem: {url}")

    response = requests.get(url, timeout=60)
    response.raise_for_status()
    raw_path.write_bytes(response.content)
    print(f"Download OK: {raw_path}")

    gcs_cp(str(raw_path), bronze_target)
    print(f"Upload bronze OK: {bronze_target}")

    df = pd.read_parquet(raw_path, engine="pyarrow")

    if df.empty:
        raise ValueError(f"Arquivo {ym} está vazio.")

    # limitar memória para teste/local
    df = df.head(500000)

    df = df.dropna()

    if df.empty:
        raise ValueError(f"Silver {ym} ficou vazia após dropna().")

    df.to_parquet(silver_path, index=False)
    print(f"Silver local OK: {silver_path}")

    gcs_cp(str(silver_path), silver_target)
    print(f"Upload silver OK: {silver_target}")

    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["pickup_date"] = df["tpep_pickup_datetime"].dt.date

    gold_df = (
        df.groupby("pickup_date")
        .agg(
            total_trips=("VendorID", "count"),
            total_revenue=("total_amount", "sum"),
        )
        .reset_index()
    )

    if gold_df.empty:
        raise ValueError(f"Gold {ym} ficou vazia.")

    gold_df.to_parquet(gold_path, index=False)
    print(f"Gold local OK: {gold_path}")

    gcs_cp(str(gold_path), gold_target)
    print(f"Upload gold OK: {gold_target}")

    if not gcs_ls(gold_target):
        raise RuntimeError(f"Validação no bucket falhou para {gold_target}")

    print(f"Validação final OK: {gold_target}")

    raw_path.unlink(missing_ok=True)
    silver_path.unlink(missing_ok=True)
    gold_path.unlink(missing_ok=True)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--start-month", type=int, required=True)
    parser.add_argument("--months-to-process", type=int, default=3)
    parser.add_argument("--max-attempts", type=int, default=12)
    parser.add_argument("--data-dir", type=str, default="/data")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    success_count = 0
    attempts = 0

    for year, month in month_iterator(args.start_year, args.start_month):
        if success_count >= args.months_to_process:
            break

        if attempts >= args.max_attempts:
            break

        ym = f"{year}-{month:02d}"

        try:
            process_month(year, month, data_dir)
            success_count += 1
            print(
                f"Sucessos acumulados: "
                f"{success_count}/{args.months_to_process}"
            )
        except Exception as e:
            print(f"Falhou para {ym}: {e}")
            print("Pulando para o próximo mês...")

        attempts += 1

    if success_count < args.months_to_process:
        raise RuntimeError(
            f"Pipeline terminou com apenas {success_count} mês(es) "
            f"processados com sucesso."
        )

    print(
        f"\nPipeline concluída com {success_count} mês(es) "
        f"processados com sucesso."
    )


if __name__ == "__main__":
    main()