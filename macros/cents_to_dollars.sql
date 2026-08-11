{% macro cents_to_dollars(column_name, scale=2) %}
    {# Converts an integer cents column to a decimal dollar value.
       Usage: {{ cents_to_dollars('amount_cents') }}
    #}
    round({{ column_name }} / 100.0, {{ scale }})
{% endmacro %}
