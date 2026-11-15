"""Pipeline ETL batch : CSV Olist -> nettoyage -> Parquet (data/processed).

Usage (depuis la racine du projet) :
    python -m src.ingestion.etl_pipeline
"""
import sys
import pandas as pd

from src.processing import cleaning
from src.processing.config import PROCESSED_DIR, RAW_DIR, REJECTED_DIR, TABLES
from src.utils.logging_config import get_logger

log = get_logger("etl")


def read(table: str, cfg: dict) -> pd.DataFrame:
    path = RAW_DIR / cfg["file"]
    if not path.exists():
        raise FileNotFoundError(f"Fichier brut introuvable : {path}")
    df = pd.read_csv(path, dtype={c: "string" for c in cfg["str_cols"]})
    log.info("[%s] lu : %d lignes, %d colonnes", table, *df.shape)
    return df


def clean(table: str, cfg: dict, df: pd.DataFrame):
    df = cleaning.standardize_columns(df)
    df = cleaning.strip_strings(df)
    df = cleaning.convert_dates(df, cfg["dates"], table)
    df, rejected = cleaning.drop_duplicates_logged(df, cfg["key"], table)
    miss = cleaning.missing_report(df)
    if not miss.empty:
        log.info("[%s] valeurs manquantes conservées (nullité légitime possible) :\n%s", table, miss.to_string())
    return df, rejected


def write(table: str, df: pd.DataFrame, rejected: pd.DataFrame) -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out = PROCESSED_DIR / f"{table}.parquet"
    df.to_parquet(out, index=False)
    check = pd.read_parquet(out)
    if len(check) != len(df):
        raise ValueError(f"[{table}] écart de lignes après écriture : {len(df)} vs {len(check)}")
    log.info("[%s] écrit : %s (%d lignes)", table, out, len(check))
    if len(rejected):
        REJECTED_DIR.mkdir(parents=True, exist_ok=True)
        rejected.to_parquet(REJECTED_DIR / f"{table}_duplicates.parquet", index=False)


def run() -> int:
    errors = 0
    for table, cfg in TABLES.items():
        try:
            raw = read(table, cfg)
            df, rejected = clean(table, cfg, raw)
            write(table, df, rejected)
        except FileNotFoundError as e:
            log.error("%s", e)
            errors += 1
        except Exception:
            log.exception("[%s] échec du traitement", table)
            errors += 1
    if errors:
        log.error("Pipeline terminé avec %d table(s) en erreur", errors)
    else:
        log.info("Pipeline terminé avec succès (%d tables)", len(TABLES))
    return errors


if __name__ == "__main__":
    sys.exit(1 if run() else 0)
