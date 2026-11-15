{{ config(materialized='table') }}

SELECT
    f.order_id,
    f.order_item_id,

    dc.customer_key,
    dp.product_key,
    ds.seller_key,

    dd_purchase.date_key AS purchase_date_key,
    dd_approved.date_key AS approved_date_key,
    dd_carrier.date_key AS carrier_delivery_date_key,
    dd_delivered.date_key AS customer_delivery_date_key,
    dd_estimated.date_key AS estimated_delivery_date_key,

    f.order_status,

    f.order_purchase_timestamp,
    f.order_approved_at,
    f.order_delivered_carrier_date,
    f.order_delivered_customer_date,
    f.order_estimated_delivery_date,

    f.price,
    f.freight_value,
    f.price + f.freight_value AS total_item_amount

FROM {{ ref('int_order_items_enriched') }} AS f

LEFT JOIN {{ ref('dim_customers') }} AS dc
    ON f.customer_id = dc.customer_id

LEFT JOIN {{ ref('dim_products') }} AS dp
    ON f.product_id = dp.product_id

LEFT JOIN {{ ref('dim_sellers') }} AS ds
    ON f.seller_id = ds.seller_id

LEFT JOIN {{ ref('dim_date') }} AS dd_purchase
    ON f.order_purchase_timestamp::DATE = dd_purchase.full_date

LEFT JOIN {{ ref('dim_date') }} AS dd_approved
    ON f.order_approved_at::DATE = dd_approved.full_date

LEFT JOIN {{ ref('dim_date') }} AS dd_carrier
    ON f.order_delivered_carrier_date::DATE = dd_carrier.full_date

LEFT JOIN {{ ref('dim_date') }} AS dd_delivered
    ON f.order_delivered_customer_date::DATE = dd_delivered.full_date

LEFT JOIN {{ ref('dim_date') }} AS dd_estimated
    ON f.order_estimated_delivery_date::DATE = dd_estimated.full_date
