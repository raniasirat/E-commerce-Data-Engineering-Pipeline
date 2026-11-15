# Rapport de profilage — Olist (données brutes)

## orders
- Fichier : `olist_orders_dataset.csv` (CSV)
- Lignes : 99,441 | Colonnes : 8
- Doublons (ligne entière) : 0
- Doublons sur la clé ['order_id'] : 0

| Colonne | Type | Manquants | % |
|---|---|---|---|
| order_id | str | 0 | 0.00 |
| customer_id | str | 0 | 0.00 |
| order_status | str | 0 | 0.00 |
| order_purchase_timestamp | str | 0 | 0.00 |
| order_approved_at | str | 160 | 0.16 |
| order_delivered_carrier_date | str | 1,783 | 1.79 |
| order_delivered_customer_date | str | 2,965 | 2.98 |
| order_estimated_delivery_date | str | 0 | 0.00 |

- Colonnes candidates aux jointures : order_id, customer_id

## customers
- Fichier : `olist_customers_dataset.csv` (CSV)
- Lignes : 99,441 | Colonnes : 5
- Doublons (ligne entière) : 0
- Doublons sur la clé ['customer_id'] : 0

| Colonne | Type | Manquants | % |
|---|---|---|---|
| customer_id | str | 0 | 0.00 |
| customer_unique_id | str | 0 | 0.00 |
| customer_zip_code_prefix | string | 0 | 0.00 |
| customer_city | str | 0 | 0.00 |
| customer_state | str | 0 | 0.00 |

- Colonnes candidates aux jointures : customer_id, customer_unique_id

## order_items
- Fichier : `olist_order_items_dataset.csv` (CSV)
- Lignes : 112,650 | Colonnes : 7
- Doublons (ligne entière) : 0
- Doublons sur la clé ['order_id', 'order_item_id'] : 0

| Colonne | Type | Manquants | % |
|---|---|---|---|
| order_id | str | 0 | 0.00 |
| order_item_id | int64 | 0 | 0.00 |
| product_id | str | 0 | 0.00 |
| seller_id | str | 0 | 0.00 |
| shipping_limit_date | str | 0 | 0.00 |
| price | float64 | 0 | 0.00 |
| freight_value | float64 | 0 | 0.00 |

- Colonnes candidates aux jointures : order_id, order_item_id, product_id, seller_id

## order_payments
- Fichier : `olist_order_payments_dataset.csv` (CSV)
- Lignes : 103,886 | Colonnes : 5
- Doublons (ligne entière) : 0
- Doublons sur la clé ['order_id', 'payment_sequential'] : 0

| Colonne | Type | Manquants | % |
|---|---|---|---|
| order_id | str | 0 | 0.00 |
| payment_sequential | int64 | 0 | 0.00 |
| payment_type | str | 0 | 0.00 |
| payment_installments | int64 | 0 | 0.00 |
| payment_value | float64 | 0 | 0.00 |

- Colonnes candidates aux jointures : order_id

## order_reviews
- Fichier : `olist_order_reviews_dataset.csv` (CSV)
- Lignes : 99,224 | Colonnes : 7
- Doublons (ligne entière) : 0
- Doublons sur la clé ['review_id', 'order_id'] : 0

| Colonne | Type | Manquants | % |
|---|---|---|---|
| review_id | str | 0 | 0.00 |
| order_id | str | 0 | 0.00 |
| review_score | int64 | 0 | 0.00 |
| review_comment_title | str | 87,656 | 88.34 |
| review_comment_message | str | 58,247 | 58.70 |
| review_creation_date | str | 0 | 0.00 |
| review_answer_timestamp | str | 0 | 0.00 |

- Colonnes candidates aux jointures : review_id, order_id

## products
- Fichier : `olist_products_dataset.csv` (CSV)
- Lignes : 32,951 | Colonnes : 9
- Doublons (ligne entière) : 0
- Doublons sur la clé ['product_id'] : 0

| Colonne | Type | Manquants | % |
|---|---|---|---|
| product_id | str | 0 | 0.00 |
| product_category_name | str | 610 | 1.85 |
| product_name_lenght | float64 | 610 | 1.85 |
| product_description_lenght | float64 | 610 | 1.85 |
| product_photos_qty | float64 | 610 | 1.85 |
| product_weight_g | float64 | 2 | 0.01 |
| product_length_cm | float64 | 2 | 0.01 |
| product_height_cm | float64 | 2 | 0.01 |
| product_width_cm | float64 | 2 | 0.01 |

- Colonnes candidates aux jointures : product_id

## sellers
- Fichier : `olist_sellers_dataset.csv` (CSV)
- Lignes : 3,095 | Colonnes : 4
- Doublons (ligne entière) : 0
- Doublons sur la clé ['seller_id'] : 0

| Colonne | Type | Manquants | % |
|---|---|---|---|
| seller_id | str | 0 | 0.00 |
| seller_zip_code_prefix | string | 0 | 0.00 |
| seller_city | str | 0 | 0.00 |
| seller_state | str | 0 | 0.00 |

- Colonnes candidates aux jointures : seller_id

## geolocation
- Fichier : `olist_geolocation_dataset.csv` (CSV)
- Lignes : 1,000,163 | Colonnes : 5
- Doublons (ligne entière) : 261,831

| Colonne | Type | Manquants | % |
|---|---|---|---|
| geolocation_zip_code_prefix | string | 0 | 0.00 |
| geolocation_lat | float64 | 0 | 0.00 |
| geolocation_lng | float64 | 0 | 0.00 |
| geolocation_city | str | 0 | 0.00 |
| geolocation_state | str | 0 | 0.00 |

- Colonnes candidates aux jointures : —

## product_category_translation
- Fichier : `product_category_name_translation.csv` (CSV)
- Lignes : 71 | Colonnes : 2
- Doublons (ligne entière) : 0
- Doublons sur la clé ['product_category_name'] : 0

| Colonne | Type | Manquants | % |
|---|---|---|---|
| product_category_name | str | 0 | 0.00 |
| product_category_name_english | str | 0 | 0.00 |

- Colonnes candidates aux jointures : —
