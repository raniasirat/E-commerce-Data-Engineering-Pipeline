{{ config(materialized='view') }}

SELECT
    review_id,
    order_id,
    review_score,
    TRIM(review_comment_title) AS review_comment_title,
    TRIM(review_comment_message) AS review_comment_message,
    review_creation_date,
    review_answer_timestamp
FROM {{ source('ecommerce_staging', 'order_reviews') }}
WHERE review_id IS NOT NULL
  AND order_id IS NOT NULL
