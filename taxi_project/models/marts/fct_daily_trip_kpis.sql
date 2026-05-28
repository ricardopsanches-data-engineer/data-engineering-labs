select
    f.pickup_date,
    d.year,
    d.month,
    d.day,
    d.day_of_week,
    d.is_weekend,

    f.total_trips,
    f.total_passengers,
    f.avg_trip_distance,
    f.avg_fare_amount,
    f.total_revenue,

    f.total_revenue / f.total_trips as revenue_per_trip,
    f.total_passengers / f.total_trips as passengers_per_trip,
    f.total_revenue / f.total_passengers as revenue_per_passenger

from {{ ref('fct_daily_trip_metrics') }} f
left join {{ ref('dim_date') }} d
    on f.pickup_date = d.date_day