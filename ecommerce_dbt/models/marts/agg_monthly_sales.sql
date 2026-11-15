{{ config(materialized='table') }}

SELECT
    DATE_TRUNC('month', order_purchase_timestamp)::DATE AS order_month,
    COUNT(DISTINCT order_id) AS nb_orders,
    COUNT(*) AS nb_items,
    SUM(price) AS revenue,
    SUM(freight_value) AS freight,
    SUM(total_item_amount) AS total_revenue
FROM {{ ref('fact_order_items') }}
GROUP BY DATE_TRUNC('month', order_purchase_timestamp)::DATE
