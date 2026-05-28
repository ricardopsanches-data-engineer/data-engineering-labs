import os
from google.cloud import storage


# =========================
# CONFIGURAÇÕES
# =========================
BUCKET_NAME = "terraform-demo-bucket-ricardo-001"
MODEL_NAME = "taxi_trips_model"
VERSION = "v20260428_071235"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

LOCAL_MODEL_DIR = os.path.join(
    BASE_DIR,
    "models",
    MODEL_NAME,
    VERSION
)

GCS_PREFIX = f"models/{MODEL_NAME}/{VERSION}"


# =========================
# UPLOAD PARA GCS
# =========================
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)

files_to_upload = [
    "model.pkl",
    "metadata.json",
]

for filename in files_to_upload:
    local_path = os.path.join(LOCAL_MODEL_DIR, filename)
    blob_path = f"{GCS_PREFIX}/{filename}"

    blob = bucket.blob(blob_path)
    blob.upload_from_filename(local_path)

    print(f"Upload concluído: gs://{BUCKET_NAME}/{blob_path}")

print("\nModelo enviado para o GCS com sucesso!")