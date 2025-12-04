"""
Simple validation script for cleaned deliveries data.
Run AFTER etl_pipeline.py has created deliveries_cleaned.csv.
"""

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

from src.config import PROCESSED_DATASET  # type: ignore


def main():
    print("=" * 60)
    print("DELIVERIES DATA - VALIDATION REPORT")
    print("=" * 60)

    if not PROCESSED_DATASET.exists():
        print(f"ERROR: {PROCESSED_DATASET} does not exist.")
        print("Run: python src/etl_pipeline.py")
        return

    df = pd.read_csv(str(PROCESSED_DATASET))
    print(f"Rows: {len(df):,}, Columns: {len(df.columns)}")
    print("\nColumns:")
    for col in df.columns:
        print("  -", col)

    # Missing values summary
    print("\nMissing values per column:")
    missing = df.isnull().sum()
    for col, cnt in missing.items():
        print(f"  {col}: {cnt}")

    # Basic stats on numeric columns
    num_cols = df.select_dtypes(include=["number"]).columns
    if len(num_cols) > 0:
        print("\nNumeric summary:")
        print(df[num_cols].describe().round(2).to_string())

    print("\nSample rows:")
    print(df.head(5).to_string(index=False))

    print("\nValidation done.")


if __name__ == "__main__":
    main()
