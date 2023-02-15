{% macro concatenate_table_columns(schema_name, relation_name,exclude_column=[]) -%}
    select 'md5('||listagg(COLUMN_NAME,' || ')||')' AS MD5Column from (
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
        {%- if exclude_column|length>0 %}
        and COLUMN_NAME NOT IN ('{{exclude_column|join("','")}}')
        {%- endif %}    
    )
{%- endmacro %}