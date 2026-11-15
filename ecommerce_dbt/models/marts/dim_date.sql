{{ config(materialized='table') }}

WITH order_dates AS (

    SELECT order_purchase_timestamp::DATE AS calendar_date
    FROM {{ ref('stg_orders') }}
    WHERE order_purchase_timestamp IS NOT NULL

    UNION ALL

    SELECT order_approved_at::DATE
    FROM {{ ref('stg_orders') }}
    WHERE order_approved_at IS NOT NULL

    UNION ALL

    SELECT order_delivered_carrier_date::DATE
    FROM {{ ref('stg_orders') }}
    WHERE order_delivered_carrier_date IS NOT NULL

    UNION ALL

    SELECT order_delivered_customer_date::DATE
    FROM {{ ref('stg_orders') }}
    WHERE order_delivered_customer_date IS NOT NULL

    UNION ALL

    SELECT order_estimated_delivery_date::DATE
    FROM {{ ref('stg_orders') }}
    WHERE order_estimated_delivery_date IS NOT NULL
),

date_range AS (
    SELECT
        MIN(calendar_date) AS min_date,
        MAX(calendar_date) AS max_date
    FROM order_dates
),

calendar AS (
    SELECT
        generate_series(
            min_date,
            max_date,
            INTERVAL '1 day'
        )::DATE AS full_date
    FROM date_range
    WHERE min_date IS NOT NULL
)

SELECT
    TO_CHAR(full_date, 'YYYYMMDD')::INTEGER AS date_key,
    full_date,
    EXTRACT(YEAR FROM full_date)::INTEGER AS year,
    EXTRACT(QUARTER FROM full_date)::INTEGER AS quarter,
    EXTRACT(MONTH FROM full_date)::INTEGER AS month,
    EXTRACT(DAY FROM full_date)::INTEGER AS day_of_month,
    EXTRACT(ISODOW FROM full_date)::INTEGER AS day_of_week,
    TRIM(TO_CHAR(full_date, 'Month')) AS month_name,
    TRIM(TO_CHAR(full_date, 'Day')) AS weekday_name,
    EXTRACT(ISODOW FROM full_date) IN (6, 7) AS is_weekend
FROM calendar
