/*
  atd specific code for generating schema names.
  This is a variation of the default and custom macros supplied with dbt
  some references
  https://docs.getdbt.com/docs/guides/debugging-schema-names
  https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-custom-schemas
  https://github.com/fishtown-analytics/dbt/blob/develop/core/dbt/include/global_project/macros/etc/get_custom_schema.sql
*/

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if (target.name == 'dev' or target.name == 'qa' or target.name == 'uat' or target.name == 'prd') and custom_schema_name is not none -%}

        {{ custom_schema_name | trim }}

    {%- else -%}

        {{ default_schema }}

    {%- endif -%}

{%- endmacro %}