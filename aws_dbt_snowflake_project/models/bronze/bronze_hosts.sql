{#- {% set incremental_flag = 1 %}
{% set incremental_col = 'CREATED_AT' %}

SELECT * FROM {{ source('staging', 'hosts') }}

{% if incremental_flag == 1 %}
    WHERE {{ incremental_col }} > (SELECT COALESCE(MAX({{ incremental_col }}), '1900-01-01') FROM {{ ref('bronze_hosts') }})
{% endif %}-#}

{{ config(materialized='incremental') }}

SELECT * FROM {{ source('staging', 'hosts') }}

{% if is_incremental() %}
    WHERE CREATED_AT > (SELECT COALESCE(MAX(CREATED_AT), '1900-01-01') FROM {{ this }})
{% endif %}
