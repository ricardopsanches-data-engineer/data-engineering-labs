import os
import time
from pathlib import Path
import requests
from requests.auth import HTTPBasicAuth

KESTRA_URL = "http://localhost:8080/api/v1/main/flows"
USERNAME = os.getenv("KESTRA_USERNAME", "")
PASSWORD = os.getenv("KESTRA_PASSWORD", "")

FLOWS_DIR = Path(r"C:\Users\sanch\data-engineering-lab\flows")

auth = HTTPBasicAuth(USERNAME, PASSWORD) if USERNAME or PASSWORD else None
headers = {"Content-Type": "application/x-yaml"}


def upsert_flow(flow_file: Path):
    namespace = None
    flow_id = None

    content = flow_file.read_text(encoding="utf-8")

    for line in content.splitlines():
        if line.startswith("namespace:"):
            namespace = line.split(":", 1)[1].strip()
        elif line.startswith("id:"):
            flow_id = line.split(":", 1)[1].strip()

        if namespace and flow_id:
            break

    if not namespace or not flow_id:
        print(f"IGNORADO: não foi possível identificar id/namespace em {flow_file.name}")
        return

    url = f"{KESTRA_URL}/{namespace}/{flow_id}"

    print(f"Sincronizando: {namespace}.{flow_id}")

    last_error = None

    for attempt in range(1, 4):
        try:
            response = requests.put(
                url,
                headers=headers,
                data=content.encode("utf-8"),
                auth=auth,
                timeout=180
            )

            if response.status_code in (200, 201):
                print(f"UPDATED: {flow_file.name}")
                return

            if response.status_code == 404:
                create_url = KESTRA_URL
                create_response = requests.post(
                    create_url,
                    headers=headers,
                    data=content.encode("utf-8"),
                    auth=auth,
                    timeout=180
                )

                if create_response.status_code in (200, 201):
                    print(f"CREATED: {flow_file.name}")
                    return
                else:
                    print(f"ERRO CREATE {create_response.status_code}: {create_response.text}: {flow_file.name}")
                    return

            print(f"ERRO UPDATE {response.status_code}: {response.text}: {flow_file.name}")
            return

        except requests.exceptions.RequestException as e:
            last_error = e
            print(f"Tentativa {attempt}/3 falhou em {flow_file.name}: {e}")
            time.sleep(5 * attempt)

    print(f"FALHA FINAL: {flow_file.name} -> {last_error}")


if __name__ == "__main__":
    allowed_files = {
        "ml_pipeline.yml",
        "ml_model_registry_pipeline.yml",
        "ml_api_prediction_pipeline.yml",
    }

    for flow_file in FLOWS_DIR.glob("*.yml"):
        if flow_file.name not in allowed_files:
            print(f"IGNORADO: {flow_file.name}")
            continue

        upsert_flow(flow_file)