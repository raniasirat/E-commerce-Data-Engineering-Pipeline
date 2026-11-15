"""Fonctions de nettoyage réutilisables"""
import re
import pandas as pd

from src.utils.logging_config import get_logger

log = get_logger(__name__)


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Noms de colonnes en snake_case minuscule."""
    df = df.copy()
    df.columns = [re.sub(r"\W+", "_", c.strip().lower()).strip("_") for c in df.columns]
    return df


def strip_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Supprime les espaces superflus et transforme les chaînes vides en NaN."""
    df = df.copy()
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].astype("string").str.strip().replace("", pd.NA)
    return df


def convert_dates(df: pd.DataFrame, date_cols: list[str], table: str) -> pd.DataFrame:
    """Convertit les dates ; les valeurs invalides deviennent NaT et sont journalisées.
    Les NaT d'origine (ex. commande non livrée) ne sont PAS considérés comme des erreurs."""
    df = df.copy()
    for col in date_cols:
        if col not in df.columns:
            log.warning("[%s] colonne date absente : %s", table, col)
            continue
        before_null = df[col].isna().sum()
        df[col] = pd.to_datetime(df[col], errors="coerce")
        newly_null = df[col].isna().sum() - before_null
        if newly_null > 0:
            log.warning("[%s] %s : %d valeurs non convertibles -> NaT", table, col, newly_null)
    return df


def drop_duplicates_logged(df: pd.DataFrame, key, table: str):
    """Supprime les doublons selon la clé métier. Retourne (df_propre, df_rejetés)."""
    if key is not None:
        missing = [k for k in key if k not in df.columns]
        if missing:
            raise KeyError(f"[{table}] colonnes de clé introuvables : {missing}")
    dup_mask = df.duplicated(subset=key, keep="first")
    rejected = df[dup_mask]
    if len(rejected):
        log.info("[%s] %d doublons supprimés (clé=%s)", table, len(rejected), key or "ligne entière")
    return df[~dup_mask].reset_index(drop=True), rejected


def missing_report(df: pd.DataFrame) -> pd.DataFrame:
    """Nombre et % de valeurs manquantes par colonne (on ne supprime rien automatiquement)."""
    n = len(df)
    rep = df.isna().sum().rename("missing").to_frame()
    rep["pct"] = (rep["missing"] / n * 100).round(2) if n else 0.0
    return rep[rep["missing"] > 0].sort_values("missing", ascending=False)
