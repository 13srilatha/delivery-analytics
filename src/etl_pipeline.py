"""
ETL pipeline for Last-Mile Delivery Analytics (Python 3.8 + pandas 1.5.3).
Reads Kaggle CSV, cleans it, creates KPI columns, saves to CSV AND PostgreSQL.
"""

import sys
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd

# Import config
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

from src.config import RAW_DATASET, PROCESSED_DATASET, DB_CONFIG  # type: ignore

# Try to import SQLAlchemy (optional for DB)
try:
    from sqlalchemy import create_engine, text
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False
    print("⚠ SQLAlchemy not installed. DB loading disabled.")


class DeliveryETL:
    def __init__(self):
        self.raw_df = None
        self.clean_df = None
        self.engine = None

    # 1. Extract
    def extract(self):
        print(f"[EXTRACT] Reading raw data from: {RAW_DATASET}")

        if not RAW_DATASET.exists():
            raise FileNotFoundError(
                f"Raw dataset not found at {RAW_DATASET}.\n"
                f"Make sure your Kaggle CSV is named 'amazon_delivery.csv' "
                f"and placed in data/raw/."
            )

        self.raw_df = pd.read_csv(str(RAW_DATASET))
        print(f"  ✓ Loaded {len(self.raw_df):,} rows, {len(self.raw_df.columns)} columns")
        print("  ✓ Columns:", list(self.raw_df.columns))
        return self

    # 2. Transform
    def transform(self):
        print("\n[TRANSFORM] Cleaning and transforming data...")
        df = self.raw_df.copy()

        initial_rows = len(df)
        print(f"  • Starting with {initial_rows:,} rows")

        # Identify likely key columns
        id_cols = [c for c in df.columns if "id" in c.lower()]
        date_cols = [c for c in df.columns if "date" in c.lower()]
        status_cols = [c for c in df.columns if "status" in c.lower()]

        # 2.1 Handle missing values
        total_missing_before = df.isnull().sum().sum()
        print(f"  • Total missing values before: {total_missing_before:,}")

        critical_cols = id_cols[:1] if id_cols else []
        if critical_cols:
            df = df.dropna(subset=critical_cols)
            print(f"  • Dropped rows with missing in {critical_cols}: {initial_rows - len(df):,}")

        # Fill remaining missing values
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col].fillna(df[col].median())
            else:
                df[col] = df[col].fillna("Unknown")

        total_missing_after = df.isnull().sum().sum()
        print(f"  ✓ Total missing values after: {total_missing_after:,}")

        # 2.2 Convert date columns
        if date_cols:
            print("  • Converting date columns to datetime:", date_cols)
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors="coerce", infer_datetime_format=True)

        # 2.3 Create time features
        if date_cols:
            main_date_col = date_cols[0]
            df["order_year"] = df[main_date_col].dt.year
            df["order_month"] = df[main_date_col].dt.month
            df["order_day_of_week"] = df[main_date_col].dt.dayofweek
            df["order_day_name"] = df[main_date_col].dt.day_name()
            print(f"  ✓ Derived time features from {main_date_col}")

        # 2.4 Create binary flags based on status
        if status_cols:
            status_col = status_cols[0]
            print(f"  • Using '{status_col}' for delivery flags")

            status_lower = df[status_col].astype(str).str.lower()
            df["is_delivered"] = status_lower.str.contains(
                "delivered|completed|success|delivered on time", na=False
            ).astype(int)
            df["is_late"] = status_lower.str.contains("late|delay", na=False).astype(int)
            df["is_on_time"] = ((df["is_delivered"] == 1) & (df["is_late"] == 0)).astype(int)
            print("  ✓ Created is_delivered, is_late, is_on_time flags")

        # 2.5 Remove duplicates
        dup_count = df.duplicated().sum()
        if dup_count > 0:
            df = df.drop_duplicates()
            print(f"  • Removed {dup_count} duplicate rows")

        final_rows = len(df)
        print(f"  ✓ Transform complete: {initial_rows:,} → {final_rows:,} rows")

        self.clean_df = df
        return self

    # 3. Validate
    def validate(self):
        print("\n[VALIDATE] Running data checks...")
        df = self.clean_df

        print(f"  • Rows: {len(df):,}, Columns: {len(df.columns)}")

        id_cols = [c for c in df.columns if "id" in c.lower()]
        if id_cols:
            unique_ratio = df[id_cols[0]].nunique() / float(len(df))
            print(f"  ✓ ID column '{id_cols[0]}' unique ratio: {unique_ratio:.2%}")

        total_cells = df.shape[0] * df.shape[1]
        total_missing = df.isnull().sum().sum()
        missing_pct = (total_missing / float(total_cells)) * 100 if total_cells else 0
        print(f"  ✓ Missing percentage: {missing_pct:.2f}%")

        return self

    # 4. Load to CSV
    def load_to_csv(self):
        print(f"\n[LOAD-CSV] Saving cleaned data to: {PROCESSED_DATASET}")
        PROCESSED_DATASET.parent.mkdir(parents=True, exist_ok=True)
        self.clean_df.to_csv(str(PROCESSED_DATASET), index=False)
        size_kb = round(PROCESSED_DATASET.stat().st_size / 1024, 1)
        print(f"  ✓ Saved {len(self.clean_df):,} rows")
        print(f"  ✓ File size: {size_kb} KB")
        return self

    # 5. Load to PostgreSQL
    def load_to_database(self):
      """Load cleaned data to PostgreSQL using direct psycopg2."""
      if not SQLALCHEMY_AVAILABLE:
          print("\n[LOAD-DB] Skipping database load (SQLAlchemy not installed)")
          return self

      print("\n[LOAD-DB] Connecting to PostgreSQL and loading data...")

      try:
          import psycopg2
          from urllib.parse import quote_plus

          # Read DB settings from config/.env
          user = DB_CONFIG["user"]
          password = DB_CONFIG["password"]  # Don't encode; psycopg2 handles it
          host = DB_CONFIG["host"]
          port = DB_CONFIG["port"]
          database = DB_CONFIG["database"]

          print(f"  • Connecting to {host}:{port}/{database}...")

          # Connect directly with psycopg2 (bypasses SQLAlchemy)
          conn = psycopg2.connect(
              host=host,
              port=port,
              user=user,
              password=password,
              database=database,
          )
          print("  ✓ Successfully connected to PostgreSQL")

          cursor = conn.cursor()

          # Drop table
          print("  • Dropping old table (if exists)...")
          cursor.execute("DROP TABLE IF EXISTS deliveries CASCADE")
          conn.commit()

          # Create table
          print("  • Creating fresh table...")
          cursor.execute("""
              CREATE TABLE deliveries (
                  Order_ID VARCHAR(100) PRIMARY KEY,
                  Agent_Age INTEGER,
                  Agent_Rating NUMERIC(5, 2),
                  Store_Latitude NUMERIC(10, 6),
                  Store_Longitude NUMERIC(10, 6),
                  Drop_Latitude NUMERIC(10, 6),
                  Drop_Longitude NUMERIC(10, 6),
                  Order_Date TIMESTAMP,
                  Order_Time VARCHAR(20),
                  Pickup_Time VARCHAR(20),
                  Weather VARCHAR(100),
                  Traffic VARCHAR(100),
                  Vehicle VARCHAR(100),
                  Area VARCHAR(100),
                  Delivery_Time INTEGER,
                  Category VARCHAR(100),
                  order_year INTEGER,
                  order_month INTEGER,
                  order_day_of_week INTEGER,
                  order_day_name VARCHAR(20),
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
              )
          """)
          conn.commit()
          print("  ✓ Table created")

          # Insert data
          print(f"  • Inserting {len(self.clean_df):,} rows into database...")
          
          # Convert dataframe to tuples and insert
          for _, row in self.clean_df.iterrows():
              placeholders = ",".join(["%s"] * len(row))
              columns = ",".join(row.index)
              insert_sql = f"INSERT INTO deliveries ({columns}) VALUES ({placeholders})"
              cursor.execute(insert_sql, tuple(row))

          conn.commit()
          print("  ✓ Data inserted")

          # Verify
          cursor.execute("SELECT COUNT(*) FROM deliveries")
          count = cursor.fetchone()[0]
          print(f"  ✓ Verified: {count:,} rows in PostgreSQL")

          cursor.close()
          conn.close()

      except Exception as e:
          print(f"  ✗ Database load failed: {e}")
          import traceback
          traceback.print_exc()

      return self

    # Run all steps
    def run(self, load_db=True):
        print("=" * 60)
        print("LAST-MILE DELIVERY ANALYTICS - ETL PIPELINE")
        print("=" * 60)
        print("Python version:", sys.version.split()[0])
        print("Pandas version:", pd.__version__)
        print("=" * 60)

        start = datetime.now()

        self.extract().transform().validate().load_to_csv()

        if load_db:
            self.load_to_database()

        duration = (datetime.now() - start).total_seconds()
        print("\n" + "=" * 60)
        print(f"✓ ETL COMPLETE in {duration:.2f} seconds")
        print(f"✓ CSV saved to: {PROCESSED_DATASET}")
        if load_db:
            print("✓ Data loaded to PostgreSQL")
        print("=" * 60)


if __name__ == "__main__":
    pipeline = DeliveryETL()
    pipeline.run(load_db=True)
