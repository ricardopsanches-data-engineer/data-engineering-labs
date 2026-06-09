import pandas as pd
from google.cloud import bigquery
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error

# =========================
# CARREGAR DADOS DO BIGQUERY
# =========================

client = bigquery.Client()

query = """
SELECT
    DATE(pickup_datetime) AS pickup_date,
    EXTRACT(DAYOFWEEK FROM pickup_datetime) AS day_of_week,
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM pickup_datetime) IN (1, 7)
        THEN TRUE ELSE FALSE
    END AS is_weekend,
    COUNT(*) AS total_trips,
    SUM(passenger_count) AS total_passengers,
    AVG(trip_distance) AS avg_trip_distance,
    SUM(fare_amount) AS total_revenue
FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2021`
WHERE pickup_datetime BETWEEN '2021-01-01' AND '2021-03-31'
GROUP BY pickup_date, day_of_week, is_weekend
ORDER BY pickup_date
"""

df = client.query(query).to_dataframe()

# =========================
# AJUSTAR TIPOS
# =========================

df["pickup_date"] = pd.to_datetime(df["pickup_date"])

numeric_cols = [
    "day_of_week",
    "total_trips",
    "total_passengers",
    "avg_trip_distance",
    "total_revenue",
]

for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")

df["is_weekend"] = df["is_weekend"].astype(int)

# remover linhas com nulos nas colunas usadas pelo modelo
df = df.dropna(
    subset=[
        "pickup_date",
        "day_of_week",
        "is_weekend",
        "total_trips",
        "total_passengers",
        "avg_trip_distance",
        "total_revenue",
    ]
).copy()

# =========================
# LIMPEZA / FILTRO FINAL
# =========================

df = df[df["pickup_date"] >= pd.Timestamp("2021-01-01")].copy()

# =========================
# FEATURES ADICIONAIS
# =========================

df["month"] = df["pickup_date"].dt.month
df["day"] = df["pickup_date"].dt.day

print("Dados carregados:")
print(df.head())
print("\nShape:", df.shape)

# =========================
# SPLIT TEMPORAL
# =========================

train_df = df[df["pickup_date"] < "2021-03-01"].copy()
test_df = df[df["pickup_date"] >= "2021-03-01"].copy()

feature_cols = [
    "day_of_week",
    "is_weekend",
    "total_trips",
    "total_passengers",
    "avg_trip_distance",
    "month",
    "day",
]

X_train = train_df[feature_cols]
y_train = train_df["total_revenue"]

X_test = test_df[feature_cols]
y_test = test_df["total_revenue"]

print("\nFeatures de treino:")
print(X_train.head())

print("\nTarget de treino:")
print(y_train.head())

print("\nPeríodo de treino:")
print(train_df["pickup_date"].min(), "até", train_df["pickup_date"].max())

print("\nPeríodo de teste:")
print(test_df["pickup_date"].min(), "até", test_df["pickup_date"].max())

# =========================
# TREINAR MODELO
# =========================

model = LinearRegression()
model.fit(X_train, y_train)

# =========================
# PREVISÃO E AVALIAÇÃO
# =========================

y_pred = model.predict(X_test)

mae = mean_absolute_error(y_test, y_pred)
mean_target = y_test.mean()
mae_pct = (mae / mean_target) * 100

print("\nMAE (erro médio absoluto):", mae)
print("Média do target no teste:", mean_target)
print("MAE percentual (%):", mae_pct)

# =========================
# COMPARAÇÃO REAL x PREVISTO
# =========================

results_df = pd.DataFrame(
    {
        "pickup_date": test_df["pickup_date"].values,
        "real_revenue": y_test.values,
        "predicted_revenue": y_pred,
    }
)

results_df["abs_error"] = (
    results_df["real_revenue"] - results_df["predicted_revenue"]
).abs()

print("\nComparação real x previsto:")
print(results_df.head(10))