{% macro surrogate_key(field_list) %}
    {#-
        Macro generates an MD5 surrogate key compatible with Trino / Presto SQL.
        Handles null substitution, UTF-8 casting, and hex-encoding.
    -#}
    lower(to_hex(md5(to_utf8(
        concat_ws('||',
            {%- for field in field_list %}
                coalesce(cast({{ field }} as varchar), '__dbt_null__')
                {%- if not loop.last %}, {% endif %}
            {%- endfor %}
        )
    ))))
{% endmacro %}
