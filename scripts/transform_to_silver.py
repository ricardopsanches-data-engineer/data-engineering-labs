import pandas as pd
from google.cloud import storage
import os

PROJECT_ID = "terraform-lab-ricardo"
BUCKET_NAME = "terraform-demo-bucket-ricardo-001"

BRONZE_FILE = "bronze/trips/yellow_tripdata_2021-01.csv"
LOCAL_FILE = "temp_yellow_tripdata.csv"
SILVER_FILE = "silver/trips/yellow_tripdata_2021-01.parquet"


def download_from_gcs():
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(BRONZE_FILE)

    blob.download_to_filename(LOCAL_FILE)
    print("Download do bronze concluído")


def transform_data():
    df = pd.read_csv(LOCAL_FILE)

    print("Shape original:", df.shape)

    # 🔹 remover duplicados
    df = df.drop_duplicates()

    # 🔹 converter datas
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    # 🔹 remover valores inválidos
    df = df[df["trip_distance"] > 0]
    df = df[df["fare_amount"] > 0]

    # 🔹 criar coluna derivada (duração da viagem)
    df["trip_duration_min"] = (
        df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
    ).dt.total_seconds() / 60

    print("Shape após limpeza:", df.shape)

    return df


def upload_to_gcs(df):
    temp_parquet = "temp.parquet"
    df.to_parquet(temp_parquet, index=False)

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(SILVER_FILE)

    blob.upload_from_filename(temp_parquet)

    print("Upload para silver concluído")


def main():
    download_from_gcs()
    df = transform_data()
    upload_to_gcs(df)


if __name__ == "__main__":
    main()