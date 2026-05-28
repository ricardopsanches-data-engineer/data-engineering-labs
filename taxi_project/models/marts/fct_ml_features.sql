{{ config(materialized='table') }}

select
    pickup_date,

    extract(dayofweek from pickup_date) as day_of_week,
    is_weekend,

    total_trips,
    total_passengers,
    avg_trip_distance,

    total_revenue

from {{ ref('fct_daily_trip_kpis') }}

where pickup_date between '2021-01-01' and '2021-01-31'