import json
from pathlib import Path

from google.cloud import bigquery


PROJECT_ID = "terraform-lab-ricardo"
DATASET = "ny_taxi"
TABLE = "model_registry_history"

POSSIBLE_LATEST_PATHS = [
    Path("/models/taxi_trips_model/latest/latest.json"),  # Kestra/Docker
    Path("C:/Users/sanch/data-engineering-lab/models/taxi_trips_model/latest/latest.json"),  # Windows local
    Path("models/taxi_trips_model/latest/latest.json"),  # relativo local
]


def find_latest_json():
    for path in POSSIBLE_LATEST_PATHS:
        if path.exists():
            return path

    raise FileNotFoundError(
        "latest.json não encontrado. Caminhos testados: "
        + " | ".join(str(path) for path in POSSIBLE_LATEST_PATHS)
    )


def get_latest_production_metric():
    client = bigquery.Client(project=PROJECT_ID)

    query = f"""
    SELECT
      candidate_metric,
      candidate_version,
      production_version,
      created_at
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE promoted = true
    ORDER BY created_at DESC
    LIMIT 1
    """

    df = client.query(query).to_dataframe()

    if df.empty:
        print("Nenhum modelo promovido encontrado no histórico.")
        return None

    production_metric = float(df.iloc[0]["candidate_metric"])
    production_version = df.iloc[0]["candidate_version"]

    print(f"Versão em produção: {production_version}")
    return production_metric


def get_candidate_metric():
    latest_path = find_latest_json()
    print(f"latest.json encontrado em: {latest_path}")

    with open(latest_path, "r", encoding="utf-8") as f:
        latest_data = json.load(f)

    print(f"Modelo candidato/latest: {latest_data.get('model_name')}")
    print(f"Versão candidata/latest: {latest_data.get('latest_version')}")

    # Temporário: até adicionarmos mae_percent no latest.json/metadata
    candidate_metric = 5.55

    return candidate_metric


def main():
    production_metric = get_latest_production_metric()
    candidate_metric = get_candidate_metric()

    print(f"Métrica produção: {production_metric}")
    print(f"Métrica candidata: {candidate_metric}")

    if production_metric is None:
        print("STATUS=APPROVED")
        print("Primeiro modelo → aprovado automaticamente")
        return

    if candidate_metric < production_metric:
        print("STATUS=APPROVED")
        print("Novo modelo MELHOR → PROMOVER")
    else:
        print("STATUS=REJECTED")
        print("ALERT_TYPE=MODEL_PERFORMANCE_DEGRADED")
        print("Novo modelo PIOR → ALERTA")
        print(f"Produção: {production_metric}")
        print(f"Candidato: {candidate_metric}")
        raise RuntimeError("Modelo rejeitado: métrica candidata pior que produção.")


if __name__ == "__main__":
    main()