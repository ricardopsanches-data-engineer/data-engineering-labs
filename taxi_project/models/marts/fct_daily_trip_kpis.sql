select
    pickup_date,
    total_trips,
    total_passengers,
    total_revenue,
    avg_fare_amount,
    avg_trip_distance,

    safe_divide(total_revenue, total_trips) as revenue_per_trip,
    safe_divide(total_passengers, total_trips) as passengers_per_trip,
    safe_divide(total_revenue, total_passengers) as revenue_per_passenger

from {{ ref('fct_daily_trip_metrics') }}