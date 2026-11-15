
import getpass
import logging
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PARQUET_DIR = PROJECT_ROOT / "data" / "processed"

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 5433,
    "dbname": "ecommerce_dw",
    "user": "ecommerce_user",
}

SCHEMA = "staging"
BATCH_SIZE = 2000

FILES = {
    "customers": "customers.parquet",
    "geolocation": "geolocation.parquet",
    "orders": "orders.parquet",
    "order_items": "order_items.parquet",
    "order_payments": "order_payments.parquet",
    "order_reviews": "order_reviews.parquet",
    "products": "products.parquet",
    "product_category_translation": "product_category_translation.parquet",
    "sellers": "sellers.parquet",
}

# Types PostgreSQL des colonnes.
# Les colonnes non mentionnées seront créées en TEXT.
TYPE_OVERRIDES = {
    "customers": {
        "customer_zip_code_prefix": "VARCHAR(10)",
    },
    "geolocation": {
        "geolocation_zip_code_prefix": "VARCHAR(10)",
        "geolocation_lat": "DOUBLE PRECISION",
        "geolocation_lng": "DOUBLE PRECISION",
    },
    "orders": {
        "order_purchase_timestamp": "TIMESTAMP",
        "order_approved_at": "TIMESTAMP",
        "order_delivered_carrier_date": "TIMESTAMP",
        "order_delivered_customer_date": "TIMESTAMP",
        "order_estimated_delivery_date": "TIMESTAMP",
    },
    "order_items": {
        "order_item_id": "INTEGER",
        "shipping_limit_date": "TIMESTAMP",
        "price": "NUMERIC(12,2)",
        "freight_value": "NUMERIC(12,2)",
    },
    "order_payments": {
        "payment_sequential": "INTEGER",
        "payment_installments": "INTEGER",
        "payment_value": "NUMERIC(12,2)",
    },
    "order_reviews": {
        "review_score": "INTEGER",
        "review_creation_date": "TIMESTAMP",
        "review_answer_timestamp": "TIMESTAMP",
    },
    "products": {
        "product_name_lenght": "DOUBLE PRECISION",
        "product_description_lenght": "DOUBLE PRECISION",
        "product_photos_qty": "DOUBLE PRECISION",
        "product_weight_g": "DOUBLE PRECISION",
        "product_length_cm": "DOUBLE PRECISION",
        "product_height_cm": "DOUBLE PRECISION",
        "product_width_cm": "DOUBLE PRECISION",
    },
    "sellers": {
        "seller_zip_code_prefix": "VARCHAR(10)",
    },
}


# --------------------------------------------------
# LOGGING
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


# --------------------------------------------------
# PREPARATION DES DONNEES
# --------------------------------------------------

def prepare_value(value):
    """Convertit les valeurs Pandas/NumPy en valeurs compatibles PostgreSQL."""
    if pd.isna(value):
        return None

    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()

    if hasattr(value, "item"):
        return value.item()

    return value


def load_dataframe(table_name, filename):
    path = PARQUET_DIR / filename

    if not path.is_file():
        raise FileNotFoundError(f"Fichier introuvable : {path}")

    df = pd.read_parquet(path)

    if df.empty:
        logging.warning("%s : fichier vide.", filename)

    if not all(isinstance(column, str) for column in df.columns):
        raise ValueError(f"Colonnes non textuelles dans {filename}")

    logging.info(
        "%s : %s lignes, %s colonnes",
        filename,
        len(df),
        len(df.columns),
    )

    return df


# --------------------------------------------------
# CREATION DES TABLES
# --------------------------------------------------

def create_table(cursor, table_name, df):
    overrides = TYPE_OVERRIDES.get(table_name, {})
    column_definitions = []

    for column in df.columns:
        pg_type = overrides.get(column, "TEXT")

        column_definitions.append(
            sql.SQL("{} {}").format(
                sql.Identifier(column),
                sql.SQL(pg_type),
            )
        )

    query = sql.SQL(
        "CREATE TABLE IF NOT EXISTS {}.{} ({})"
    ).format(
        sql.Identifier(SCHEMA),
        sql.Identifier(table_name),
        sql.SQL(", ").join(column_definitions),
    )

    cursor.execute(query)


# --------------------------------------------------
# CHARGEMENT DES DONNEES
# --------------------------------------------------

def insert_dataframe(cursor, table_name, df):
    if df.empty:
        return

    columns = list(df.columns)

    insert_query = sql.SQL(
        "INSERT INTO {}.{} ({}) VALUES %s"
    ).format(
        sql.Identifier(SCHEMA),
        sql.Identifier(table_name),
        sql.SQL(", ").join(
            sql.Identifier(column) for column in columns
        ),
    )

    records = (
        tuple(prepare_value(value) for value in row)
        for row in df.itertuples(index=False, name=None)
    )

    execute_values(
        cursor,
        insert_query.as_string(cursor.connection),
        records,
        page_size=BATCH_SIZE,
    )


# --------------------------------------------------
# PROGRAMME PRINCIPAL
# --------------------------------------------------

def main():
    password = getpass.getpass("Mot de passe PostgreSQL : ")
    DB_CONFIG["password"] = password

    logging.info("Connexion à PostgreSQL...")

    connection = psycopg2.connect(**DB_CONFIG)

    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                        sql.Identifier(SCHEMA)
                    )
                )

                for table_name, filename in FILES.items():
                    logging.info("Traitement de la table %s...", table_name)

                    df = load_dataframe(table_name, filename)

                    # Création si la table n'existe pas.
                    create_table(cursor, table_name, df)

                    # Remplacement des données de staging.
                    cursor.execute(
                        sql.SQL("TRUNCATE TABLE {}.{}").format(
                            sql.Identifier(SCHEMA),
                            sql.Identifier(table_name),
                        )
                    )

                    insert_dataframe(cursor, table_name, df)

                    cursor.execute(
                        sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                            sql.Identifier(SCHEMA),
                            sql.Identifier(table_name),
                        )
                    )

                    loaded_count = cursor.fetchone()[0]

                    if loaded_count != len(df):
                        raise RuntimeError(
                            f"Écart pour {table_name} : "
                            f"Parquet={len(df)}, PostgreSQL={loaded_count}"
                        )

                    logging.info(
                        "OK %s : %s lignes chargées et vérifiées.",
                        table_name,
                        loaded_count,
                    )

        logging.info("CHARGEMENT TERMINE AVEC SUCCES.")

    finally:
        connection.close()


if __name__ == "__main__":
    main()
