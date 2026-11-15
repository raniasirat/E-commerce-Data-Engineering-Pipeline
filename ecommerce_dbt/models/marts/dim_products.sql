{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY product_id)::INTEGER AS product_key,
    product_id,
    product_category_name,
    product_category_name_english,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
FROM {{ ref('int_products_enriched') }}
