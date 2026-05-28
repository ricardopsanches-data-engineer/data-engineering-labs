import os
import json
from google.cloud import storage


BUCKET_NAME = "terraform-demo-bucket-ricardo-001"
MODEL_NAME = "taxi_trips_model"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCAL_MODEL_DIR = os.path.join(BASE_DIR, "models", MODEL_NAME, "latest")

os.makedirs(LOCAL_MODEL_DIR, exist_ok=True)

client = storage.Client()
bucket = client.bucket(BUCKET_NAME)

latest_blob_path = f"models/{MODEL_NAME}/latest.json"

latest_blob = bucket.blob(latest_blob_path)
latest_content = latest_blob.download_as_text()

latest = json.loads(latest_content)

latest_version = latest["latest_version"]
gcs_model_path = latest["gcs_model_path"]

print(f"Modelo latest encontrado: {latest_version}")
print(f"Modelo no GCS: {gcs_model_path}")

model_blob_path = f"models/{MODEL_NAME}/{latest_version}/model.pkl"

local_model_path = os.path.join(LOCAL_MODEL_DIR, "model.pkl")
local_latest_path = os.path.join(LOCAL_MODEL_DIR, "latest.json")

bucket.blob(model_blob_path).download_to_filename(local_model_path)

with open(local_latest_path, "w", encoding="utf-8") as f:
    json.dump(latest, f, indent=4, ensure_ascii=False)

print(f"Modelo baixado para: {local_model_path}")
print(f"latest.json salvo em: {local_latest_path}")