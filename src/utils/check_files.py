"""Tâche 1 du DAG : vérifie que les CSV bruts attendus sont présents et non vides.
Code de sortie 1 si un fichier manque -> la tâche Airflow passe en échec."""
import sys

from src.processing.config import RAW_DIR, TABLES
from src.utils.logging_config import get_logger

log = get_logger("check_files")


def main() -> int:
    problems = []
    for name, cfg in TABLES.items():
        path = RAW_DIR / cfg["file"]
        if not path.exists():
            problems.append(f"{name}: fichier absent ({path})")
        elif path.stat().st_size == 0:
            problems.append(f"{name}: fichier vide ({path})")
        else:
            log.info("OK  %-30s %8.1f Ko", cfg["file"], path.stat().st_size / 1024)
    if problems:
        for problem in problems:
            log.error(problem)
        return 1
    log.info("Tous les fichiers bruts sont présents (%d)", len(TABLES))
    return 0


if __name__ == "__main__":
    sys.exit(main())
