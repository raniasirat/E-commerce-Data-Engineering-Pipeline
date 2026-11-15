"""Profilage des tables brutes  -> docs/profiling_report.md

Usage : python -m src.processing.profiling
"""
import pandas as pd

from src.processing.config import RAW_DIR, TABLES
from src.utils.logging_config import get_logger

log = get_logger(__name__)


def profile_table(name: str, cfg: dict) -> str:
    path = RAW_DIR / cfg["file"]
    if not path.exists():
        log.error("Fichier absent : %s", path)
        return f"## {name}\n\n_Fichier absent : `{path}`_\n"
    df = pd.read_csv(path, dtype={c: "string" for c in cfg["str_cols"]})
    lines = [
        f"## {name}",
        f"- Fichier : `{cfg['file']}` (CSV)",
        f"- Lignes : {len(df):,} | Colonnes : {df.shape[1]}",
        f"- Doublons (ligne entière) : {df.duplicated().sum():,}",
    ]
    if cfg["key"]:
        lines.append(f"- Doublons sur la clé {cfg['key']} : {df.duplicated(subset=cfg['key']).sum():,}")
    lines += ["", "| Colonne | Type | Manquants | % |", "|---|---|---|---|"]
    for col in df.columns:
        miss = int(df[col].isna().sum())
        lines.append(f"| {col} | {df[col].dtype} | {miss:,} | {miss / len(df) * 100:.2f} |")
    keys = [c for c in df.columns if c.endswith("_id")]
    lines += ["", f"- Colonnes candidates aux jointures : {', '.join(keys) or '—'}", ""]
    return "\n".join(lines)


def main() -> None:
    parts = ["# Rapport de profilage — Olist (données brutes)\n"]
    for name, cfg in TABLES.items():
        log.info("Profilage de %s", name)
        parts.append(profile_table(name, cfg))
    out = "docs/profiling_report.md"
    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(parts))
    log.info("Rapport écrit : %s", out)


if __name__ == "__main__":
    main()
