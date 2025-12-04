"""
Configuration for delivery analytics project (Python 3.8 friendly).
Uses .env if available, but works fine without DB for now.
"""

import os
from pathlib import Path

# Try to load environment variables if python-dotenv is installed
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # Not critical; DB is optional
    pass

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent

# Data directories
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"

# Ensure directories exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

# File paths
RAW_DATASET = RAW_DATA_DIR / "amazon_delivery.csv"   # your Kaggle file
PROCESSED_DATASET = PROCESSED_DATA_DIR / "deliveries_cleaned.csv"

# Optional DB config (NOT used now, only if you enable DB later)
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "delivery_analytics"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# Basic KPI thresholds (for validation / business logic)
KPI_THRESHOLDS = {
    "on_time_delivery_target": 0.95,        # 95%
    "max_acceptable_delay_hours": 24.0,
    "cost_per_delivery_target": 8.50,
}

# Debug print once when imported (helps you check paths quickly)
if __name__ == "__main__":
    print("Project root:", PROJECT_ROOT)
    print("RAW_DATA_DIR:", RAW_DATA_DIR)
    print("PROCESSED_DATA_DIR:", PROCESSED_DATA_DIR)
    print("RAW_DATASET exists:", RAW_DATASET.exists())
    if RAW_DATASET.exists():
        print("RAW_DATASET size (KB):", round(RAW_DATASET.stat().st_size / 1024, 1))
