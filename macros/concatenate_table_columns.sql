{% macro concatenate_table_columns(schema_name='SRC', relation_name='EMPLOYEE') -%}
    select 
        'nvl(cast('||COLUMN_NAME|| ' as varchar),'''')' as COLUMN_NAME
        from           
        {% if target.name == "default" -%} dbt_dev 
        {%- elif target.name == "dev" -%} dbt_dev 
        {%- elif target.name == "qa" -%} dbt_qa 
        {%- elif target.name == "prd" -%} dbt_prd 
        {%- else -%}  
        {%- endif %}.information_schema.columns 
        where table_schema='{{schema_name}}' and table_name='{{relation_name}}'

{%- endmacro %}