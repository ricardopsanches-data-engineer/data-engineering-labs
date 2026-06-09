from pathlib import Path
import joblib
from sklearn.ensemble import RandomForestRegressor
from google.cloud import bigquery
import pandas as pd

PROJECT_ID = "terraform-lab-ricardo"

client = bigquery.Client(project=PROJECT_ID)

query = """
SELECT *
FROM `terraform-lab-ricardo.ny_taxi.features_daily_trip_metrics`
WHERE total_trips IS NOT NULL
"""

df = client.query(query).to_dataframe()

features = [
    "day_of_week",
    "is_weekend",
    "month",
    "day",
    "total_passengers",
    "avg_trip_distance",
    "avg_fare_amount",
    "lag_1_total_trips",
    "lag_7_total_trips",
    "rolling_avg_7_days",
    "rolling_avg_14_days"
]

target = "total_trips"

X = df[features]
y = df[target]

model = RandomForestRegressor(
    n_estimators=100,
    random_state=42
)

model.fit(X, y)

output_dir = Path(
    r"C:\Users\sanch\data-engineering-lab\models\vertex_export"
)

output_dir.mkdir(parents=True, exist_ok=True)

joblib.dump(model, output_dir / "model.joblib")

print("Modelo exportado com sucesso.")