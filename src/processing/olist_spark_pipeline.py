""" pipeline PySpark batch (CSV -> nettoyage -> agrégats -> Parquet).

Usage : python -m src.processing.olist_spark_pipeline
Sorties : data/processed/spark/<table> et data/processed/spark/agg_*
"""
import sys
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

from src.processing import spark_transformations as T
from src.processing.spark_session import get_spark
from src.utils.logging_config import get_logger

log = get_logger("spark_pipeline")

RAW_DIR = Path("data/raw")
OUT_DIR = Path("data/processed/spark")
PANDAS_DIR = Path("data/processed")


def read_table(spark: SparkSession, name: str, cfg: dict) -> DataFrame:
    path = RAW_DIR / cfg["file"]
    if not path.exists():
        raise FileNotFoundError(f"Fichier brut introuvable : {path}")
    return spark.read.option("header", True).option("inferSchema", False).csv(str(path))


def write_parquet(spark: SparkSession, df: DataFrame, name: str) -> int:
    out = str(OUT_DIR / name)
    expected = df.count()
    df.write.mode("overwrite").parquet(out)
    back = spark.read.parquet(out)
    got, ncols = back.count(), len(back.columns)
    if got != expected or ncols != len(df.columns):
        raise ValueError(
            f"[{name}] contrôle échoué : {expected} lignes/{len(df.columns)} col. "
            f"attendues, {got}/{ncols} relues"
        )
    log.info("[%s] écrit : %d lignes, %d colonnes -> %s", name, got, ncols, out)
    return got


def compare_with_pandas(spark_counts: dict) -> None:
    """Compare les lignes Spark avec les Parquet Pandas de la semaine 1, s'ils existent."""
    for table, n_spark in spark_counts.items():
        path = PANDAS_DIR / f"{table}.parquet"
        if not path.exists():
            log.info("[%s] pas de sortie Pandas à comparer", table)
            continue
        n_pandas = len(pd.read_parquet(path))
        status = "OK" if n_pandas == n_spark else "ÉCART"
        log.info("[%s] comparaison Pandas=%d | Spark=%d -> %s", table, n_pandas, n_spark, status)


def run() -> int:
    spark = get_spark("OlistPipeline")
    errors, counts, clean = 0, {}, {}
    try:
        for name, cfg in T.SPARK_TABLES.items():
            try:
                df = T.clean_table(read_table(spark, name, cfg), cfg)
                if name == "order_items":
                    df = T.filter_valid_order_items(df)
                elif name == "order_payments":
                    df = T.filter_valid_payments(df)
                counts[name] = write_parquet(spark, df, name)
                clean[name] = spark.read.parquet(str(OUT_DIR / name))
            except FileNotFoundError as error:
                log.error("%s", error)
                errors += 1
            except Exception:
                log.exception("[%s] échec", name)
                errors += 1

        try:
            if {"orders", "customers", "order_items"} <= clean.keys():
                enriched = T.orders_enriched(
                    clean["orders"], clean["customers"], clean["order_items"]
                )
                write_parquet(spark, T.monthly_sales(enriched), "agg_monthly_sales")
            if "orders" in clean:
                write_parquet(
                    spark, T.orders_by_status(clean["orders"]), "agg_orders_by_status"
                )
            if {"order_items", "products", "product_category_translation"} <= clean.keys():
                write_parquet(
                    spark,
                    T.sales_by_category(
                        clean["order_items"],
                        clean["products"],
                        clean["product_category_translation"],
                    ),
                    "agg_sales_by_category",
                )
        except Exception:
            log.exception("échec des agrégats")
            errors += 1

        compare_with_pandas(counts)
        log.info("Pipeline Spark terminé : %d erreur(s)", errors)
        return errors
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(1 if run() else 0)
