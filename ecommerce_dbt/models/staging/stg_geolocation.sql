{{ config(materialized='view') }}

SELECT
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    TRIM(geolocation_city) AS geolocation_city,
    UPPER(TRIM(geolocation_state)) AS geolocation_state
FROM {{ source('ecommerce_staging', 'geolocation') }}
