{% snapshot dim_user_history %}

    {{
        config(
        unique_key='USER_KEY',
        strategy='check',
        check_cols=['MD5_COLUMN'],
        )
    }}

select 
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
from {{ ref('dim_user') }} as stg

{% endsnapshot %}