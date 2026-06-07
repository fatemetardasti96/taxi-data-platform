with trips as (
    select * from {{ ref('base_trip') }}
)

select 
    pickup_weather_category,
    pickup_datetime,
    pickup_zone,
    round(sum(trips.fare_amount), 2) as revenue,
    round(sum(trips.trip_distance), 2) as trip_distance,
    count(*) as trip_count
from trips
group by all