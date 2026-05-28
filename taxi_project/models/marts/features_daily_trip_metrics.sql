{{ config(
    materialized='table'
) }}

WITH base AS (

    SELECT
        pickup_date,
        total_trips,
        total_passengers,
        avg_trip_distance,
        avg_fare_amount,
        total_revenue
    FROM {{ ref('fct_daily_trip_metrics') }}

),

features AS (

    SELECT
        pickup_date,

        -- calendário
        EXTRACT(DAYOFWEEK FROM pickup_date) AS day_of_week,

        CASE
            WHEN EXTRACT(DAYOFWEEK FROM pickup_date) IN (1, 7) THEN 1
            ELSE 0
        END AS is_weekend,

        EXTRACT(MONTH FROM pickup_date) AS month,
        EXTRACT(DAY FROM pickup_date) AS day,

        -- métricas atuais
        total_trips,
        total_passengers,
        avg_trip_distance,
        avg_fare_amount,
        total_revenue,

        --LAG FEATURES (MUITO IMPORTANTES)
        LAG(total_trips, 1) OVER (ORDER BY pickup_date) AS lag_1_total_trips,
        LAG(total_trips, 7) OVER (ORDER BY pickup_date) AS lag_7_total_trips,

        --MÉDIAS MÓVEIS
        AVG(total_trips) OVER (
            ORDER BY pickup_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_avg_7_days,

        AVG(total_trips) OVER (
            ORDER BY pickup_date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS rolling_avg_14_days

    FROM base

)

SELECT *
FROM features