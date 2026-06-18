/*
  KPI 2: Weather impact on demand and revenue vs clear/cloudy baseline per zone.
  trip_count_uplift_pct and revenue_uplift_pct express % change vs clear_or_cloudy.
*/
with trips as (
    select * from {{ ref('base_trip') }}
    where pickup_weather_category is not null
      and pickup_zone is not null
),

by_weather_zone as (
    select
        pickup_weather_category as weather_category,
        pickup_zone,
        count(*) as trip_count,
        round(sum(total_amount), 2) as revenue,
        round(avg(trip_distance), 4) as avg_trip_distance,
        round(
            avg(epoch(dropoff_datetime - pickup_datetime) / 60.0),
            2
        ) as avg_trip_duration_minutes,
        round(avg(total_amount), 2) as avg_revenue_per_trip
    from trips
    group by pickup_weather_category, pickup_zone
),

baseline as (
    select
        pickup_zone,
        trip_count as baseline_trip_count,
        revenue as baseline_revenue,
        avg_trip_distance as baseline_avg_trip_distance,
        avg_trip_duration_minutes as baseline_avg_trip_duration_minutes,
        avg_revenue_per_trip as baseline_avg_revenue_per_trip
    from by_weather_zone
    where weather_category = 'clear_or_cloudy'
)

select
    w.weather_category,
    w.pickup_zone,
    w.trip_count,
    w.revenue,
    w.avg_trip_distance,
    w.avg_trip_duration_minutes,
    w.avg_revenue_per_trip,
    b.baseline_trip_count,
    b.baseline_revenue,
    round(
        case
            when b.baseline_trip_count > 0
            then (w.trip_count / b.baseline_trip_count - 1) * 100
            else null
        end,
        2
    ) as trip_count_uplift_pct,
    round(
        case
            when b.baseline_revenue > 0
            then (w.revenue / b.baseline_revenue - 1) * 100
            else null
        end,
        2
    ) as revenue_uplift_pct
from by_weather_zone as w
left join baseline as b on w.pickup_zone = b.pickup_zone
