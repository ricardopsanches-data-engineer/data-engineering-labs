from google.cloud import storage

# Nome do bucket criado pelo Terraform
BUCKET_NAME = "terraform-demo-bucket-ricardo-001"

# Lista de arquivos para upload
FILES = [
    {
        "local_path": r"C:\Users\sanch\data-engineering-lab\data\yellow_tripdata_2021-01.csv",
        "gcs_path": "bronze/trips/yellow_tripdata_2021-01.csv",
    },
    {
        "local_path": r"C:\Users\sanch\data-engineering-lab\data\taxi_zone_lookup.csv",
        "gcs_path": "bronze/zones/taxi_zone_lookup.csv",
    },
]

def upload_file_to_gcs(bucket_name: str, local_path: str, gcs_path: str) -> None:
    """Uploads a local file to a Google Cloud Storage bucket."""
    client = storage.Client(project="terraform-lab-ricardo")
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)

    blob.upload_from_filename(local_path)
    print(f"Upload concluído: {local_path} -> gs://{bucket_name}/{gcs_path}")

def main() -> None:
    for file_info in FILES:
        upload_file_to_gcs(
            bucket_name=BUCKET_NAME,
            local_path=file_info["local_path"],
            gcs_path=file_info["gcs_path"],
        )


if __name__ == "__main__":
    main()