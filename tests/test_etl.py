import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

from src.config import RAW_DATASET, PROCESSED_DATASET  # type: ignore
from src.etl_pipeline import DeliveryETL  # type: ignore


def test_etl_creates_output():
    """Basic test: ETL runs and produces a non-empty cleaned CSV."""
    assert RAW_DATASET.exists(), "Raw dataset file is missing"

    pipeline = DeliveryETL()
    pipeline.extract().transform().validate().load()

    assert PROCESSED_DATASET.exists(), "Processed dataset was not created"

    df = pd.read_csv(str(PROCESSED_DATASET))
    assert len(df) > 0, "Processed dataset is empty"
