with weather as (
    select * from {{ ref('stg_weather') }}
),

weather_code_mapping as (
    select * from {{ ref('weather_code_mapping') }}
)

select
    weather.latitude,
    weather.longitude,
    weather.timestamp,
    weather.weather_code,
    weather_code_mapping.description as weather,
    weather_code_mapping.category as weather_category
from weather
left join weather_code_mapping on weather.weather_code = weather_code_mapping.code