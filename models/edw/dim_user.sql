{{
    config(
        materialized= 'incremental'
        ,unique_key='USER_KEY'
        ,incremental_strategy='merge'
        ,pre_hook='{{ udpate_stg_dim_user_timestamp() }}'
    )
}}

WITH stg as (
    SELECT 
    md5(stg.EMPLOYEE_ID) AS USER_KEY
    ,stg.EMPLOYEE_ID
    ,stg.EMPLOYEE_NAME
    ,stg.DEPARTMENT_ID
    ,stg.EMAIL
    ,stg.PHONE
    ,stg.ADDRESS
    ,stg.HIRE_DATE
    ,stg.EMPLOYMENT_STATUS
    ,stg.MD5_COLUMN
    ,stg.SNOW_INSERT_TIME
    ,stg.SNOW_UPDATE_TIME
    FROM {{ ref('stg_dim_user')}} stg 
{% if is_incremental() %}
    WHERE (stg.SNOW_INSERT_TIME > (SELECT MAX(SNOW_INSERT_TIME) FROM {{ this }}))
    OR 
    (stg.SNOW_UPDATE_TIME > (SELECT MAX(SNOW_UPDATE_TIME) FROM {{ this }}))
{% endif %}    
)
SELECT 
stg.USER_KEY
,stg.EMPLOYEE_ID
,stg.EMPLOYEE_NAME
,stg.DEPARTMENT_ID
,stg.EMAIL
,stg.PHONE
,stg.ADDRESS
,stg.HIRE_DATE
,stg.EMPLOYMENT_STATUS
,stg.MD5_COLUMN
,stg.SNOW_INSERT_TIME
,stg.SNOW_UPDATE_TIME
FROM stg 
LEFT JOIN {{ this }} dim 
ON nvl(stg.EMPLOYEE_ID,0)=nvl(dim.EMPLOYEE_ID,0)