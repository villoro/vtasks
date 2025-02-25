{% macro gsheet_to_numeric(column) -%}
    REGEXP_EXTRACT({{ column }}, '([\d-.,]+)').replace('.', '').replace(',', '.').nullif('')
{%- endmacro %}
