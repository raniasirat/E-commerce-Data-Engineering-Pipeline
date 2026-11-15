"""Règles par table du dataset Olist : fichier, clé métier, dates, types."""
from pathlib import Path

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
REJECTED_DIR = PROCESSED_DIR / "rejected"

# name -> fichier CSV, clé de dédoublonnage (règle métier), colonnes date, colonnes à lire en texte
TABLES = {
    "orders": {
        "file": "olist_orders_dataset.csv",
        "key": ["order_id"],
        "dates": [
            "order_purchase_timestamp", "order_approved_at",
            "order_delivered_carrier_date", "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
        "str_cols": [],
    },
    "customers": {
        "file": "olist_customers_dataset.csv",
        "key": ["customer_id"],
        "dates": [],
        "str_cols": ["customer_zip_code_prefix"],  # garder les zéros initiaux
    },
    "order_items": {
        "file": "olist_order_items_dataset.csv",
        "key": ["order_id", "order_item_id"],
        "dates": ["shipping_limit_date"],
        "str_cols": [],
    },
    "order_payments": {
        "file": "olist_order_payments_dataset.csv",
        "key": ["order_id", "payment_sequential"],
        "dates": [],
        "str_cols": [],
    },
    "order_reviews": {
        "file": "olist_order_reviews_dataset.csv",
        # review_id n'est pas unique à lui seul dans Olist
        "key": ["review_id", "order_id"],
        "dates": ["review_creation_date", "review_answer_timestamp"],
        "str_cols": [],
    },
    "products": {
        "file": "olist_products_dataset.csv",
        "key": ["product_id"],
        "dates": [],
        "str_cols": [],
    },
    "sellers": {
        "file": "olist_sellers_dataset.csv",
        "key": ["seller_id"],
        "dates": [],
        "str_cols": ["seller_zip_code_prefix"],
    },
    "geolocation": {
        "file": "olist_geolocation_dataset.csv",
        "key": None,  # beaucoup de doublons exacts : on supprime les lignes identiques
        "dates": [],
        "str_cols": ["geolocation_zip_code_prefix"],
    },
    "product_category_translation": {
        "file": "product_category_name_translation.csv",
        "key": ["product_category_name"],
        "dates": [],
        "str_cols": [],
    },
}
