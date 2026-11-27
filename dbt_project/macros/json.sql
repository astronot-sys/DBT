{#- Cross-database JSON helpers, so marts run unchanged on Postgres and Databricks. -#}

{#- Text value of a top-level JSON key, e.g. json_text('custom_fields', 'CountryOfManufacture'). -#}
{% macro json_text(column, key) -%}
    {{ return(adapter.dispatch('json_text')(column, key)) }}
{%- endmacro %}

{% macro default__json_text(column, key) -%}
    ({{ column }})::jsonb ->> '{{ key }}'
{%- endmacro %}

{% macro databricks__json_text(column, key) -%}
    get_json_object({{ column }}, '$.{{ key }}')
{%- endmacro %}


{#- A top-level JSON key holding an array of strings, as a native array. -#}
{% macro json_string_array(column, key) -%}
    {{ return(adapter.dispatch('json_string_array')(column, key)) }}
{%- endmacro %}

{% macro default__json_string_array(column, key) -%}
    array(select jsonb_array_elements_text(({{ column }})::jsonb -> '{{ key }}'))
{%- endmacro %}

{% macro databricks__json_string_array(column, key) -%}
    from_json(get_json_object({{ column }}, '$.{{ key }}'), 'array<string>')
{%- endmacro %}
