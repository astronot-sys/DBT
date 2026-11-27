{#-
  Use the custom schema name exactly (staging, marts) instead of public_staging.
  On Databricks the schemas share the `workspace` catalog with other work, so they
  get a `wwi_` prefix there (wwi_staging, wwi_marts). The raw source schema follows
  the same rule in models/staging/_sources.yml.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.type == 'databricks' -%}
        wwi_{{ custom_schema_name | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
