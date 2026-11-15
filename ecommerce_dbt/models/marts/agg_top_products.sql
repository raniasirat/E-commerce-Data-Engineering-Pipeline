{{ config(materialized='table') }}

SELECT
    dp.product_key,
    dp.product_id,
    dp.product_category_name,
    dp.product_category_name_english,
    COUNT(*) AS nb_items,
    COUNT(DISTINCT f.order_id) AS nb_orders,
    SUM(f.price) AS revenue,
    SUM(f.freight_value) AS freight,
    SUM(f.total_item_amount) AS total_revenue
FROM {{ ref('fact_order_items') }} AS f
JOIN {{ ref('dim_products') }} AS dp
    ON f.product_key = dp.product_key
GROUP BY
    dp.product_key,
    dp.product_id,
    dp.product_category_name,
    dp.product_category_name_english
ORDER BY total_revenue DESC
