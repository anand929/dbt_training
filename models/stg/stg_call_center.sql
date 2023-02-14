--select * from {{ source('src','call_center_test') }}
WITH cte as (
    {{ concatenate_table_columns('SRC','RULES') }}
)
select 'md5('||listagg(COLUMN_NAME,' || ')||')' AS MD5_Column from cte