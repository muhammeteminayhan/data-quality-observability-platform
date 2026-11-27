{#
    Make a model's `+schema` an absolute schema name rather than a suffix.

    dbt's default generate_schema_name concatenates the target schema and the
    custom one, so a model configured with `+schema: staging` against target
    schema `public` lands in `public_staging`. This project's contracts, the
    GX suites and sql/source/20_dbt_schemas.sql all name `staging` and
    `analytics` exactly, so the default would silently build the models
    somewhere nothing is looking.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
