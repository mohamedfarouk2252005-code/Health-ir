from pathlib import Path


# =========================
# PROJECT PATHS
# =========================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data" / "healthcare"

OUTPUT_DIR = PROJECT_ROOT / "output"


# =========================
# DATASET CONFIGURATION
# =========================

NUM_DOCUMENTS = 10

DOCUMENT_IDS = [
    f"d{i}" for i in range(1, NUM_DOCUMENTS + 1)
]


# =========================
# CALCULATION CONFIGURATION
# =========================

LOG_BASE = 10


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
