import argparse
from pathlib import Path
import urllib.request


def main():
    parser = argparse.ArgumentParser(description="Baixa arquivo Yellow Taxi para Bronze")
    parser.add_argument("--year", required=True, help="Ano com 4 dígitos, ex: 2021")
    parser.add_argument("--month", required=True, help="Mês com 2 dígitos, ex: 02")
    args = parser.parse_args()

    year = args.year
    month = args.month

    file_name = f"yellow_tripdata_{year}-{month}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"

    bronze_dir = Path("/data/bronze/trips")
    bronze_dir.mkdir(parents=True, exist_ok=True)

    output_path = bronze_dir / file_name

    print(f"Baixando: {url}")
    print(f"Destino: {output_path}")

    try:
        urllib.request.urlretrieve(url, output_path)
    except Exception as e:
        raise RuntimeError(f"Falha ao baixar arquivo de {url}: {e}")

    if not output_path.exists():
        raise FileNotFoundError(f"Download não gerou arquivo em: {output_path}")

    print("Download concluído com sucesso.")


if __name__ == "__main__":
    main()