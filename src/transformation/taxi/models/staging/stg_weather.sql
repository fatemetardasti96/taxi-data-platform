with weather as (
    select * from {{ source('bronze', 'weather') }}
)
select
    cast(latitude as double) as latitude,
    cast(longitude as double) as longitude,
    cast(timestamp as timestamp) as timestamp,
    cast(weather_code as integer) as weather_code
from weather