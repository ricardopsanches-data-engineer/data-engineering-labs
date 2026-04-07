select
    pickup_date,
    any_value(total_trips) as total_trips,
    any_value(total_passengers) as total_passengers,
    any_value(avg_trip_distance) as avg_trip_distance,
    any_value(avg_fare_amount) as avg_fare_amount,
    any_value(total_revenue) as total_revenue

from {{ source('ny_taxi_source', 'daily_trip_metrics_auto') }}

group by pickup_date