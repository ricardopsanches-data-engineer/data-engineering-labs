with base as (

    select *
    from {{ ref('stg_daily_trip_metrics') }}

),

daily_metrics as (

    select
        pickup_date,
        total_trips,
        total_passengers,
        total_revenue,
        avg_trip_distance,
        avg_fare_amount,

        -- métricas derivadas
        total_revenue / nullif(total_trips, 0) as revenue_per_trip,
        total_passengers / nullif(total_trips, 0) as passengers_per_trip,
        total_revenue / nullif(total_passengers, 0) as revenue_per_passenger,

        -- diferença de receita vs dia anterior
        total_revenue
            - lag(total_revenue) over (order by pickup_date) as revenue_diff_day,

        -- variação percentual diária
        (
            total_revenue
            - lag(total_revenue) over (order by pickup_date)
        ) / nullif(lag(total_revenue) over (order by pickup_date), 0) as revenue_pct_change,

        -- média móvel 7 dias
        avg(total_revenue) over (
            order by pickup_date
            rows between 6 preceding and current row
        ) as rolling_avg_7d_revenue,

        -- dia da semana
        extract(dayofweek from pickup_date) as day_of_week,

        -- z-score simples
        (
            total_revenue - avg(total_revenue) over ()
        ) / nullif(stddev(total_revenue) over (), 0) as revenue_z_score

    from base

),

final as (

    select
        *,

        -- receita semanal
        sum(total_revenue) over (
            partition by extract(isoyear from pickup_date), extract(isoweek from pickup_date)
        ) as weekly_revenue

    from daily_metrics

)

select
    *,

    -- crescimento semanal vs semana anterior
    (
        weekly_revenue
        - lag(weekly_revenue) over (
            order by extract(isoyear from pickup_date), extract(isoweek from pickup_date)
        )
    ) / nullif(
        lag(weekly_revenue) over (
            order by extract(isoyear from pickup_date), extract(isoweek from pickup_date)
        ),
        0
    ) as weekly_revenue_growth

from final