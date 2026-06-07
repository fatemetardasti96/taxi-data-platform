{% macro normalize_timestamp(column_name) %}
coalesce(
    try_cast({{ column_name }} as timestamp),
    try_strptime(cast({{ column_name }} as varchar), '%Y-%m-%d %H:%M:%S'),
    try_strptime(cast({{ column_name }} as varchar), '%Y-%m-%d %H:%M:%S.%f'),
    try_strptime(cast({{ column_name }} as varchar), '%Y-%m-%d %H:%M:%S.%g')
)
{% endmacro %}
