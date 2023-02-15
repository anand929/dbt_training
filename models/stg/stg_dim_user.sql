{# Below call statement uses a macro to return concatenated columns for a specified table #}
{%- call statement('linkresult', fetch_result=True) -%}
    {{ concatenate_table_columns('SRC','EMPLOYEE',['EMPLOYEE_ID']) }}
{%- endcall -%}
{%- set var_MD5Column = load_result('linkresult')['data'][0][0] -%}

WITH CTE AS (
    SELECT 
    EMPLOYEE_ID
    ,EMPLOYEE_NAME
    ,DEPARTMENT_ID
    ,EMAIL
    ,PHONE
    ,ADDRESS
    ,HIRE_DATE
    ,EMPLOYMENT_STATUS
    FROM {{ source('src','employee')}}
)
SELECT 
    CAST(EMPLOYEE_ID AS INTEGER) AS EMPLOYEE_ID
    ,EMPLOYEE_NAME
    ,CAST(DEPARTMENT_ID AS INTEGER) AS DEPARTMENT_ID
    ,EMAIL
    ,CAST(PHONE AS INTEGER) AS PHONE
    ,ADDRESS
    ,HIRE_DATE
    ,EMPLOYMENT_STATUS 
    --,md5(nvl(cast(EMPLOYEE_NAME AS VARCHAR()),'')||nvl(cast(DEPARTMENT_ID AS VARCHAR()),'')||nvl(cast(EMAIL AS VARCHAR()),'')||nvl(cast(PHONE AS VARCHAR()),'')||nvl(cast(ADDRESS AS VARCHAR()),'')||nvl(cast(HIRE_DATE AS VARCHAR()),'')||nvl(cast(EMPLOYMENT_STATUS AS VARCHAR()),'')) as MD5_COLUMN
    ,{{ var_MD5Column }} AS MD5_COLUMN
    ,CURRENT_TIMESTAMP AS SNOW_INSERT_TIME
    ,CURRENT_TIMESTAMP AS SNOW_UPDATE_TIME
FROM CTE