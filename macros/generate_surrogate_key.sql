{% macro generate_surrogate_key(field_list) %}
    {# Generates an MD5 surrogate key from one or more columns.
       Usage: {{ generate_surrogate_key(['customer_id', 'order_id']) }}
    #}
    md5(
        concat_ws('|',
            {% for field in field_list %}
                coalesce(cast({{ field }} as string), 'NULL')
                {%- if not loop.last %},{% endif %}
            {% endfor %}
        )
    )
{% endmacro %}
