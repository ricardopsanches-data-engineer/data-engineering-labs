with dates as (

    select distinct
        pickup_date as date_day
    from {{ ref('stg_daily_trip_metrics') }}

)

select
    date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day,
    extract(dayofweek from date_day) as day_of_week,
    case
        when extract(dayofweek from date_day) in (1, 7) then true
        else false
    end as is_weekend
from dates