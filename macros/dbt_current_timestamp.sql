{% macro dbt_current_timestamp() %}
    {# Returns the current UTC timestamp.
       Wraps current_timestamp() for cross-adapter compatibility.
    #}
    current_timestamp()
{% endmacro %}
