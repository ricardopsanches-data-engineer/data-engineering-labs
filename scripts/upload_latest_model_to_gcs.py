import os
import json
from datetime import datetime, timezone

from google.cloud import storage
from google.cloud import bigquery


# =========================
# CONFIGURAÇÕES
# =========================
PROJECT_ID = "terraform-lab-ricardo"
BUCKET_NAME = "terraform-demo-bucket-ricardo-001"
MODEL_NAME = "taxi_trips_model"
METRIC_NAME = "mae_percent"
MIN_IMPROVEMENT_PERCENT = 1.0

BQ_HISTORY_TABLE = "terraform-lab-ricardo.ny_taxi.model_registry_history"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCAL_MODEL_BASE_DIR = os.path.join(BASE_DIR, "models", MODEL_NAME)

storage_client = storage.Client(project=PROJECT_ID)
bucket = storage_client.bucket(BUCKET_NAME)


# =========================
# FUNÇÕES AUXILIARES
# =========================
def load_json_from_gcs(blob_path):
    blob = bucket.blob(blob_path)

    if not blob.exists():
        return None

    return json.loads(blob.download_as_text())


def upload_file(local_path, gcs_path):
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)
    print(f"Upload concluído: gs://{BUCKET_NAME}/{gcs_path}")


def save_history_to_bigquery(
    candidate_version,
    production_version,
    candidate_metric,
    production_metric,
    improvement_percent,
    promoted,
    reason,
):
    bq_client = bigquery.Client(project=PROJECT_ID)

    row = [{
        "model_name": MODEL_NAME,
        "candidate_version": candidate_version,
        "production_version": production_version,
        "metric_name": METRIC_NAME,
        "candidate_metric": float(candidate_metric),
        "production_metric": float(production_metric) if production_metric is not None else None,
        "min_improvement_percent": float(MIN_IMPROVEMENT_PERCENT),
        "improvement_percent": float(improvement_percent) if improvement_percent is not None else None,
        "promoted": bool(promoted),
        "reason": reason,
        "candidate_model_path": f"gs://{BUCKET_NAME}/models/{MODEL_NAME}/{candidate_version}/model.pkl",
        "candidate_metadata_path": f"gs://{BUCKET_NAME}/models/{MODEL_NAME}/{candidate_version}/metadata.json",
        "production_model_path": (
            f"gs://{BUCKET_NAME}/models/{MODEL_NAME}/{production_version}/model.pkl"
            if production_version else None
        ),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }]

    errors = bq_client.insert_rows_json(BQ_HISTORY_TABLE, row)

    if errors:
        raise Exception(f"Erro ao salvar histórico no BigQuery: {errors}")

    print(f"Histórico salvo no BigQuery: {BQ_HISTORY_TABLE}")


# =========================
# IDENTIFICAR NOVA VERSÃO LOCAL
# =========================
versions = [
    d for d in os.listdir(LOCAL_MODEL_BASE_DIR)
    if d.startswith("v") and os.path.isdir(os.path.join(LOCAL_MODEL_BASE_DIR, d))
]

if not versions:
    raise Exception(f"Nenhuma versão encontrada em {LOCAL_MODEL_BASE_DIR}")

new_version = sorted(versions)[-1]
new_version_dir = os.path.join(LOCAL_MODEL_BASE_DIR, new_version)

new_model_path = os.path.join(new_version_dir, "model.pkl")
new_metadata_path = os.path.join(new_version_dir, "metadata.json")

if not os.path.exists(new_model_path):
    raise FileNotFoundError(f"Modelo não encontrado: {new_model_path}")

if not os.path.exists(new_metadata_path):
    raise FileNotFoundError(f"Metadata não encontrado: {new_metadata_path}")

with open(new_metadata_path, "r", encoding="utf-8") as f:
    new_metadata = json.load(f)

new_metric = new_metadata["metrics"][METRIC_NAME]

print(f"Nova versão encontrada: {new_version}")
print(f"Métrica nova ({METRIC_NAME}): {new_metric:.4f}")


# =========================
# UPLOAD DA NOVA VERSÃO
# =========================
upload_file(
    new_model_path,
    f"models/{MODEL_NAME}/{new_version}/model.pkl"
)

upload_file(
    new_metadata_path,
    f"models/{MODEL_NAME}/{new_version}/metadata.json"
)


# =========================
# LER MODELO ATUAL EM PRODUÇÃO
# =========================
latest_blob_path = f"models/{MODEL_NAME}/latest.json"
latest = load_json_from_gcs(latest_blob_path)

promote = False
reason = ""
current_version = None
current_metric = None
improvement_percent = None

if latest is None:
    promote = True
    reason = "Nenhum latest.json existente. Primeira promoção."
else:
    current_version = latest["latest_version"]
    current_metadata_path = f"models/{MODEL_NAME}/{current_version}/metadata.json"

    current_metadata = load_json_from_gcs(current_metadata_path)

    if current_metadata is None:
        promote = True
        reason = "Metadata da versão atual não encontrada. Promovendo nova versão."
    else:
        current_metric = current_metadata["metrics"][METRIC_NAME]

        print(f"Versão atual em produção: {current_version}")
        print(f"Métrica atual ({METRIC_NAME}): {current_metric:.4f}")

        required_metric = current_metric * (1 - MIN_IMPROVEMENT_PERCENT / 100)
        improvement_percent = ((current_metric - new_metric) / current_metric) * 100

        if new_metric < required_metric:
            promote = True
            reason = (
                f"Modelo novo melhorou {METRIC_NAME} em {improvement_percent:.2f}%: "
                f"atual={current_metric:.4f}, novo={new_metric:.4f}, "
                f"mínimo exigido={required_metric:.4f}"
            )
        else:
            promote = False
            reason = (
                f"Modelo novo não atingiu melhoria mínima de {MIN_IMPROVEMENT_PERCENT:.2f}%: "
                f"atual={current_metric:.4f}, novo={new_metric:.4f}, "
                f"melhoria={improvement_percent:.2f}%, "
                f"mínimo exigido={required_metric:.4f}"
            )


# =========================
# PROMOTION
# =========================
if promote:
    latest_json = {
        "model_name": MODEL_NAME,
        "latest_version": new_version,
        "gcs_model_path": f"gs://{BUCKET_NAME}/models/{MODEL_NAME}/{new_version}/model.pkl",
        "gcs_metadata_path": f"gs://{BUCKET_NAME}/models/{MODEL_NAME}/{new_version}/metadata.json",
        "promotion_metric": METRIC_NAME,
        "promotion_metric_value": new_metric,
        "min_improvement_percent": MIN_IMPROVEMENT_PERCENT,
        "improvement_percent": improvement_percent,
        "promotion_reason": reason,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }

    latest_local_path = os.path.join(LOCAL_MODEL_BASE_DIR, "latest.json")

    with open(latest_local_path, "w", encoding="utf-8") as f:
        json.dump(latest_json, f, indent=4, ensure_ascii=False)

    upload_file(
        latest_local_path,
        latest_blob_path
    )

    print("\nPROMOTION APROVADA")
    print(reason)
    print(f"latest.json atualizado para: {new_version}")

else:
    print("\nPROMOTION REJEITADA")
    print(reason)
    print(f"A versão {new_version} foi salva no registry, mas NÃO virou latest.")


# =========================
# HISTÓRICO NO BIGQUERY
# =========================
save_history_to_bigquery(
    candidate_version=new_version,
    production_version=current_version,
    candidate_metric=new_metric,
    production_metric=current_metric,
    improvement_percent=improvement_percent,
    promoted=promote,
    reason=reason,
)