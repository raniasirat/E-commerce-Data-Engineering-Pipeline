{{ config(materialized='view') }}

SELECT
    order_id,
    customer_id,
    LOWER(TRIM(order_status)) AS order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
FROM {{ source('ecommerce_staging', 'orders') }}
WHERE order_id IS NOT NULL
