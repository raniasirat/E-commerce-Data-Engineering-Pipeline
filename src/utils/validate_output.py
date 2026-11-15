"""Tâche 4 du DAG : valide les sorties Parquet (Pandas et Spark). Code 1 si anomalie."""
import sys
from pathlib import Path

import pandas as pd

from src.processing.config import TABLES
from src.utils.logging_config import get_logger

log = get_logger("validate_output")

PANDAS_DIR = Path("data/processed")
SPARK_DIR = PANDAS_DIR / "spark"
SPARK_TABLES = [
    "orders",
    "customers",
    "order_items",
    "order_payments",
    "products",
    "sellers",
    "product_category_translation",
]
SPARK_AGGS = [
    "agg_monthly_sales",
    "agg_orders_by_status",
    "agg_sales_by_category",
]


def check(path: Path, label: str, errors: list) -> int:
    if not path.exists():
        errors.append(f"{label}: sortie absente ({path})")
        return -1
    n = len(pd.read_parquet(path))
    if n == 0:
        errors.append(f"{label}: 0 ligne")
    else:
        log.info("OK  %-40s %9d lignes", label, n)
    return n


def main() -> int:
    errors = []
    pandas_counts = {
        table: check(PANDAS_DIR / f"{table}.parquet", f"pandas/{table}", errors)
        for table in TABLES
    }
    spark_counts = {
        table: check(SPARK_DIR / table, f"spark/{table}", errors)
        for table in SPARK_TABLES
    }
    for aggregate in SPARK_AGGS:
        check(SPARK_DIR / aggregate, f"spark/{aggregate}", errors)

    # Tables sans filtre métier côté Spark : les comptes doivent être identiques.
    for table in ["orders", "customers", "products", "sellers"]:
        if (
            pandas_counts.get(table, -1) > 0
            and spark_counts.get(table, -1) > 0
            and pandas_counts[table] != spark_counts[table]
        ):
            errors.append(
                f"{table}: Pandas={pandas_counts[table]} != Spark={spark_counts[table]}"
            )

    if errors:
        for error in errors:
            log.error(error)
        return 1
    log.info("Validation OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
