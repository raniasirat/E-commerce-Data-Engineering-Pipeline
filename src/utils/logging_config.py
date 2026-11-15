
"""Configuration centralisée des logs (console + fichier)."""

import logging
from pathlib import Path


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
    )

    # Logs dans la console : visibles dans Airflow
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)

    # Dossier de logs partagé avec Airflow
    log_dir = Path("/opt/airflow/logs/ecommerce")
    log_dir.mkdir(parents=True, exist_ok=True)

    # Logs dans un fichier
    log_file = log_dir / "etl.log"

    file_handler = logging.FileHandler(
        log_file,
        encoding="utf-8"
    )
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    return logger
