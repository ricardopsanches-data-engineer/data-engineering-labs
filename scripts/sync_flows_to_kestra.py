from pathlib import Path
import time
import requests
import yaml

KESTRA_URL = "http://localhost:8080"
USERNAME = "admin@kestra.io"
PASSWORD = "Admin1234"
FLOWS_DIR = Path("C:/Users/sanch/data-engineering-lab/flows")
# se for rodar dentro do container, troque para:
# FLOWS_DIR = Path("/flows")

session = requests.Session()
session.auth = (USERNAME, PASSWORD)
session.headers.update({"Content-Type": "application/x-yaml"})


def upsert_flow(content: str, namespace: str, flow_id: str) -> str:
    update_url = f"{KESTRA_URL}/api/v1/main/flows/{namespace}/{flow_id}"
    create_url = f"{KESTRA_URL}/api/v1/main/flows"

    for attempt in range(1, 4):
        try:
            response = session.put(update_url, data=content.encode("utf-8"), timeout=60)

            if response.status_code in (200, 204):
                return "UPDATED"

            if response.status_code == 404:
                response = session.post(create_url, data=content.encode("utf-8"), timeout=60)
                if response.ok:
                    return "CREATED"
                return f"ERRO CREATE {response.status_code}: {response.text}"

            return f"ERRO UPDATE {response.status_code}: {response.text}"

        except requests.exceptions.RequestException as e:
            print(f"tentativa {attempt}/3 falhou: {e}")
            if attempt < 3:
                time.sleep(3)
            else:
                return f"ERRO CONEXAO: {e}"


def main():
    yaml_files = sorted(FLOWS_DIR.glob("*.yaml"))

    if not yaml_files:
        print(f"Nenhum flow encontrado em {FLOWS_DIR}")
        return

    for file in yaml_files:
        try:
            content = file.read_text(encoding="utf-8")
            flow = yaml.safe_load(content)

            flow_id = flow["id"]
            namespace = flow["namespace"]

            print(f"Sincronizando: {namespace}.{flow_id}")
            result = upsert_flow(content, namespace, flow_id)
            print(f"{result}: {file.name}")

        except Exception as e:
            print(f"ERRO LOCAL: {file.name} -> {e}")


if __name__ == "__main__":
    main()