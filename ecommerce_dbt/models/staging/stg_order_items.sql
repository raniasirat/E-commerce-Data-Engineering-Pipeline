{{ config(materialized='view') }}

SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value
FROM {{ source('ecommerce_staging', 'order_items') }}
WHERE order_id IS NOT NULL
  AND order_item_id IS NOT NULL
