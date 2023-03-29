{% macro udpate_stg_dim_user_timestamp() %}
    update {{ref('stg_dim_user')}}
    SET SNOW_INSERT_TIME=CURRENT_TIMESTAMP
    ,SNOW_UPDATE_TIME=CURRENT_TIMESTAMP
{% endmacro %}