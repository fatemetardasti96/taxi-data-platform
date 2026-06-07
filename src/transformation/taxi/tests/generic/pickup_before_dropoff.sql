{% test pickup_before_dropoff(model) %}
select *
from {{ model }}
where pickup_datetime > dropoff_datetime
{% endtest %}
