with trips as (
    select *
    from {{ ref('stg_taxi_trips') }}
),
zones as (
    select * from {{ ref('taxi_zone_lookup') }}
)

select
    trips.vendor_id,
    trips.pickup_datetime,
    trips.dropoff_datetime,
    trips.passenger_count,
    trips.trip_distance,
    trips.rate_code_id,
    trips.store_and_fwd_flag,
    trips.pickup_location_id,
    trips.dropoff_location_id,
    trips.payment_type,
    trips.fare_amount,
    trips.extra,
    trips.mta_tax,
    trips.tip_amount,
    trips.tolls_amount,
    trips.improvement_surcharge,
    trips.total_amount,
    trips.congestion_surcharge,
    trips.airport_fee,
    zones.Zone as pickup_zone,
    dropoff_zones.Zone as dropoff_zone,
    zones.latitude as pickup_latitude,
    zones.longitude as pickup_longitude,
    dropoff_zones.latitude as dropoff_latitude,
    dropoff_zones.longitude as dropoff_longitude
from trips
left join zones on trips.pickup_location_id = zones.LocationId
left join zones as dropoff_zones on trips.dropoff_location_id = dropoff_zones.LocationId