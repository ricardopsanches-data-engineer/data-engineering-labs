import json
import os
from pathlib import Path

REQUIRED_FIELDS = [
    "model_name",
    "latest_version",
    "gcs_model_path",
    "gcs_metadata_path",
    "updated_at",
]

POSSIBLE_PATHS = [
    Path("C:/Users/sanch/data-engineering-lab/models/taxi_trips_model/latest/latest.json"),
    Path("/models/taxi_trips_model/latest/latest.json"),
    Path("/data/models/taxi_trips_model/latest/latest.json"),
    Path("/scripts/../models/taxi_trips_model/latest/latest.json"),
    Path("models/taxi_trips_model/latest/latest.json"),
]

def find_latest_path():
    env_path = os.getenv("LATEST_JSON_PATH")

    if env_path and Path(env_path).exists():
        return Path(env_path)

    for path in POSSIBLE_PATHS:
        if path.exists():
            return path

    raise FileNotFoundError(
        "latest.json não encontrado em nenhum caminho esperado: "
        + ", ".join(str(p) for p in POSSIBLE_PATHS)
    )

def main():
    latest_path = find_latest_path()

    print(f"latest.json encontrado em: {latest_path}")

    with open(latest_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    missing_fields = [field for field in REQUIRED_FIELDS if field not in data]

    if missing_fields:
        raise ValueError(f"latest.json inválido. Campos ausentes: {missing_fields}")

    if not data["gcs_model_path"].endswith("/model.pkl"):
        raise ValueError("gcs_model_path inválido: deve apontar para model.pkl")

    if not data["gcs_metadata_path"].endswith("/metadata.json"):
        raise ValueError("gcs_metadata_path inválido: deve apontar para metadata.json")

    print("latest.json validado com sucesso!")
    print(f"Modelo: {data['model_name']}")
    print(f"Versão latest: {data['latest_version']}")
    print(f"Model path: {data['gcs_model_path']}")
    print(f"Metadata path: {data['gcs_metadata_path']}")
    print(f"Atualizado em: {data['updated_at']}")


if __name__ == "__main__":
    main()