{{ config(materialized='table') }}

SELECT
    order_status,
    COUNT(DISTINCT order_id) AS nb_orders,
    COUNT(*) AS nb_items
FROM {{ ref('fact_order_items') }}
GROUP BY order_status
