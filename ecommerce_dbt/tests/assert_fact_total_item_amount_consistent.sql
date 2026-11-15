SELECT
    order_id,
    order_item_id,
    price,
    freight_value,
    total_item_amount
FROM {{ ref('fact_order_items') }}
WHERE total_item_amount IS DISTINCT FROM (price + freight_value)
