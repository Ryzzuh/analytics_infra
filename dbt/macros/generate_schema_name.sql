{#
  dbt's default appends the model's custom schema to the target schema, which would give
  `staging_staging` / `staging_core`. The platform's layers ARE schemas (SPEC.md §5.1), so a
  model's configured schema is used verbatim and only unconfigured models fall back to target.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
