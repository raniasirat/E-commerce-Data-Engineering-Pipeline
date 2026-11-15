SELECT
    order_id,
    order_item_id,
    price,
    freight_value
FROM {{ ref('fact_order_items') }}
WHERE price < 0
   OR freight_value < 0
