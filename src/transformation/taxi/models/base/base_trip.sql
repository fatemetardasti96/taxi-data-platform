with trips as (
    select * from {{ ref('prep_taxi_trips') }}
),

weather as (
    select * from {{ ref('prep_weather') }}
)

select
    trips.pickup_datetime,
    trips.dropoff_datetime,
    trips.passenger_count,
    trips.trip_distance,
    trips.pickup_location_id,
    trips.dropoff_location_id,
    trips.pickup_zone,
    trips.dropoff_zone,
    trips.fare_amount,
    trips.tolls_amount,
    trips.total_amount,
    pickup_weather.weather as pickup_weather,
    pickup_weather.weather_category as pickup_weather_category,
    dropoff_weather.weather as dropoff_weather,
    dropoff_weather.weather_category as dropoff_weather_category
from trips
left join weather pickup_weather on date_trunc('hour', trips.pickup_datetime) = pickup_weather.timestamp
and trips.pickup_latitude = pickup_weather.latitude
and trips.pickup_longitude = pickup_weather.longitude
left join weather dropoff_weather on date_trunc('hour', trips.dropoff_datetime) = dropoff_weather.timestamp
and trips.dropoff_latitude = dropoff_weather.latitude
and trips.dropoff_longitude = dropoff_weather.longitude