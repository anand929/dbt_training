--select * from {{ source('src','call_center_test') }}
WITH concatcte as (
    {{ concatenate_table_columns('SRC','RULES',['RULE_NUMBER']) }}
)

select 'md5('||listagg(COLUMN_NAME,' || ')||')' AS MD5_Column from concatcte