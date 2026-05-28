import mlflow
from mlflow.tracking import MlflowClient


MODEL_NAME = "taxi_trips_model"
PRODUCTION_ALIAS = "production"
STAGING_ALIAS = "staging"


def main():
    client = MlflowClient()

    versions = client.search_model_versions(f"name = '{MODEL_NAME}'")

    if not versions:
        raise ValueError(f"Nenhuma versão encontrada para o modelo: {MODEL_NAME}")

    latest_version = max(versions, key=lambda v: int(v.version))

    print(f"Modelo: {MODEL_NAME}")
    print(f"Última versão encontrada: v{latest_version.version}")
    print(f"Run ID: {latest_version.run_id}")
    print(f"Status: {latest_version.status}")

    client.set_registered_model_alias(
        name=MODEL_NAME,
        alias=STAGING_ALIAS,
        version=latest_version.version,
    )

    print(f"Alias '{STAGING_ALIAS}' apontando para versão {latest_version.version}")

    client.set_registered_model_alias(
        name=MODEL_NAME,
        alias=PRODUCTION_ALIAS,
        version=latest_version.version,
    )

    print(f"Alias '{PRODUCTION_ALIAS}' apontando para versão {latest_version.version}")

    client.set_model_version_tag(
        name=MODEL_NAME,
        version=latest_version.version,
        key="promoted_to",
        value=PRODUCTION_ALIAS,
    )

    print("Promoção concluída com sucesso.")


if __name__ == "__main__":
    main()