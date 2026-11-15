"""Jour 9 : exploration Spark de orders, customers, order_items.

Usage : python -m src.processing.spark_explore
"""
from src.processing.spark_session import get_spark

FILES = {
    "orders": "data/raw/olist_orders_dataset.csv",
    "customers": "data/raw/olist_customers_dataset.csv",
    "order_items": "data/raw/olist_order_items_dataset.csv",
}


def main() -> None:
    spark = get_spark("Day9-Explore")
    try:
        for name, path in FILES.items():
            print(f"\n===== {name} =====")
            df = spark.read.option("header", True).option("inferSchema", True).csv(path)
            df.printSchema()
            print("lignes :", df.count())
            df.show(5, truncate=False)
            df.describe().show()

        orders = spark.read.option("header", True).csv(FILES["orders"])
        delivered = orders.filter(orders.order_status == "delivered")
        print("commandes livrées :", delivered.count())
        delivered.explain()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
