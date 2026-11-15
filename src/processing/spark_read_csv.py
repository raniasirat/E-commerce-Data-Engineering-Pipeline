"""
 première lecture CSV avec PySpark + comparaison Pandas / Spark.

Usage : python -m src.processing.spark_read_csv
"""
import pandas as pd
from pyspark.sql import functions as F

from src.processing.spark_session import get_spark

PATH = "data/raw/olist_orders_dataset.csv"


def main() -> None:
    spark = get_spark("Day8-ReadCSV")
    try:
        print("PySpark OK, version Spark :", spark.version)

        sdf = spark.read.option("header", True).csv(PATH)
        print("Spark - nb lignes :", sdf.count())
        sdf.groupBy("order_status").count().orderBy(F.desc("count")).show()

        pdf = pd.read_csv(PATH)
        print("Pandas - nb lignes :", len(pdf))
        print(pdf["order_status"].value_counts())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
