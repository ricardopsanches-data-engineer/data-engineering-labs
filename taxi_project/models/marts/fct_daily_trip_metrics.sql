{{
    config(
        materialized='incremental',
        unique_key='pickup_date'
    )
}}

select
    pickup_date,
    total_trips,
    total_passengers,
    avg_trip_distance,
    avg_fare_amount,
    total_revenue
from {{ ref('stg_daily_trip_metrics') }}

{% if is_incremental() %}
where pickup_date >= date_sub((select max(pickup_date) from {{ this }}), interval 7 day)
{% endif %}