{{ config(materialized='view') }}

SELECT
    order_id,
    payment_sequential,
    LOWER(TRIM(payment_type)) AS payment_type,
    payment_installments,
    payment_value
FROM {{ source('ecommerce_staging', 'order_payments') }}
WHERE order_id IS NOT NULL
  AND payment_sequential IS NOT NULL
