{{ config(
    materialized='incremental',
    unique_key='pickup_date',
    incremental_strategy='merge',
    partition_by={
      "field": "pickup_date",
      "data_type": "date"
    }
) }}

WITH source_data AS (

    SELECT
        pickup_date,
        total_trips,
        total_passengers,
        avg_trip_distance,
        avg_fare_amount,
        total_revenue
    FROM {{ ref('stg_daily_trip_metrics') }}

    {% if is_incremental() %}
    WHERE pickup_date > (
        SELECT MAX(pickup_date)
        FROM {{ this }}
    )
    {% endif %}

)

SELECT
    pickup_date,
    total_trips,
    total_passengers,
    avg_trip_distance,
    avg_fare_amount,
    total_revenue
FROM source_data