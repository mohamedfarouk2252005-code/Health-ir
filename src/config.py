from pathlib import Path


# =========================
# PROJECT PATHS
# =========================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Default dataset folder (the user can select another one at run time)
DEFAULT_DATA_DIR = PROJECT_ROOT / "data" / "healthcare"

OUTPUT_DIR = PROJECT_ROOT / "output"


# =========================
# ASSIGNED QUERIES
# =========================

PHRASE_QUERIES = [
    "patient care",
    "medical research"
]

BOOLEAN_QUERIES = [
    "patient care AND medical research",
    "patient care AND NOT medical research"
]