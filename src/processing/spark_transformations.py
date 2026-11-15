"""Jour 10 : transformations PySpark réutilisables (DataFrame -> DataFrame)."""
import re

from pyspark.sql import DataFrame, functions as F

SPARK_TABLES = {
    "orders": {
        "file": "olist_orders_dataset.csv",
        "key": ["order_id"],
        "timestamps": [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
        "ints": [],
        "doubles": [],
    },
    "customers": {
        "file": "olist_customers_dataset.csv",
        "key": ["customer_id"],
        "timestamps": [],
        "ints": [],
        "doubles": [],
    },
    "order_items": {
        "file": "olist_order_items_dataset.csv",
        "key": ["order_id", "order_item_id"],
        "timestamps": ["shipping_limit_date"],
        "ints": ["order_item_id"],
        "doubles": ["price", "freight_value"],
    },
    "order_payments": {
        "file": "olist_order_payments_dataset.csv",
        "key": ["order_id", "payment_sequential"],
        "timestamps": [],
        "ints": ["payment_sequential", "payment_installments"],
        "doubles": ["payment_value"],
    },
    "products": {
        "file": "olist_products_dataset.csv",
        "key": ["product_id"],
        "timestamps": [],
        "ints": [
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ],
        "doubles": [],
    },
    "sellers": {
        "file": "olist_sellers_dataset.csv",
        "key": ["seller_id"],
        "timestamps": [],
        "ints": [],
        "doubles": [],
    },
    "product_category_translation": {
        "file": "product_category_name_translation.csv",
        "key": ["product_category_name"],
        "timestamps": [],
        "ints": [],
        "doubles": [],
    },
}


def standardize_columns(df: DataFrame) -> DataFrame:
    for column in df.columns:
        new_column = re.sub(r"\W+", "_", column.strip().lower()).strip("_")
        if new_column != column:
            df = df.withColumnRenamed(column, new_column)
    return df


def trim_strings(df: DataFrame) -> DataFrame:
    """Trimpe les chaînes et transforme les chaînes vides en NULL."""
    for name, dtype in df.dtypes:
        if dtype == "string":
            trimmed = F.trim(F.col(name))
            df = df.withColumn(name, F.when(trimmed == "", None).otherwise(trimmed))
    return df


def cast_columns(
    df: DataFrame,
    timestamps: tuple[str, ...] | list[str] = (),
    ints: tuple[str, ...] | list[str] = (),
    doubles: tuple[str, ...] | list[str] = (),
) -> DataFrame:
    for column in timestamps:
        df = df.withColumn(column, F.to_timestamp(F.col(column)))
    for column in ints:
        df = df.withColumn(column, F.col(column).cast("int"))
    for column in doubles:
        df = df.withColumn(column, F.col(column).cast("double"))
    return df


def clean_table(df: DataFrame, cfg: dict) -> DataFrame:
    """Nettoyage, typage, filtre des clés nulles et dédoublonnage par clé métier."""
    df = standardize_columns(df)
    df = trim_strings(df)
    df = cast_columns(df, cfg["timestamps"], cfg["ints"], cfg["doubles"])
    for key in cfg["key"]:
        df = df.filter(F.col(key).isNotNull())
    return df.dropDuplicates(cfg["key"])


def filter_valid_order_items(df: DataFrame) -> DataFrame:
    """Conserve les articles dont le prix et les frais de port sont non négatifs."""
    return df.filter(
        F.col("price").isNotNull()
        & (F.col("price") >= 0)
        & F.col("freight_value").isNotNull()
        & (F.col("freight_value") >= 0)
    )


def filter_valid_payments(df: DataFrame) -> DataFrame:
    return df.filter(F.col("payment_value").isNotNull() & (F.col("payment_value") >= 0))


def orders_enriched(orders: DataFrame, customers: DataFrame, items: DataFrame) -> DataFrame:
    """Joint orders et customers (customer_id), puis order_items (order_id)."""
    return orders.join(customers, "customer_id", "left").join(items, "order_id", "left")


def monthly_sales(enriched: DataFrame) -> DataFrame:
    return (
        enriched.withColumn(
            "order_month", F.date_format("order_purchase_timestamp", "yyyy-MM")
        )
        .groupBy("order_month")
        .agg(
            F.countDistinct("order_id").alias("nb_orders"),
            F.round(F.sum("price"), 2).alias("revenue"),
            F.round(F.sum("freight_value"), 2).alias("freight"),
        )
        .orderBy("order_month")
    )


def orders_by_status(orders: DataFrame) -> DataFrame:
    return orders.groupBy("order_status").count().orderBy(F.desc("count"))


def sales_by_category(
    items: DataFrame, products: DataFrame, translation: DataFrame
) -> DataFrame:
    return (
        items.join(products.select("product_id", "product_category_name"), "product_id", "left")
        .join(translation, "product_category_name", "left")
        .withColumn(
            "category",
            F.coalesce(
                "product_category_name_english",
                "product_category_name",
                F.lit("unknown"),
            ),
        )
        .groupBy("category")
        .agg(F.count("*").alias("nb_items"), F.round(F.sum("price"), 2).alias("revenue"))
        .orderBy(F.desc("revenue"))
    )
