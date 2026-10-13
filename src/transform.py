
from pathlib import Path

import pandas as pd

from config.pipeline_config import (
    PROCESSED_DATA_PATH,
    RAW_DATA_PATH,
)


def transform_data() -> dict:
    """Clean taxi data and write a processed Parquet file."""
    source_path = Path(RAW_DATA_PATH)
    output_path = Path(PROCESSED_DATA_PATH)

    if not source_path.exists():
        raise FileNotFoundError(
            f"Raw data not found: {source_path}"
        )

    df = pd.read_parquet(source_path)
    source_rows = len(df)

    # Convert date columns to proper datetime values.
    for column in [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
    ]:
        df[column] = pd.to_datetime(
            df[column], errors="coerce"
        )

    # Convert numeric columns safely.
    for column in [
        "trip_distance",
        "fare_amount",
        "total_amount",
    ]:
        df[column] = pd.to_numeric(
            df[column], errors="coerce"
        )

    # Mark records that fail our basic quality rules.
    invalid = (
        df["tpep_pickup_datetime"].isna()
        | df["tpep_dropoff_datetime"].isna()
        | df["trip_distance"].isna()
        | df["fare_amount"].isna()
        | df["total_amount"].isna()
        | (
            df["tpep_dropoff_datetime"]
            <= df["tpep_pickup_datetime"]
        )
        | (df["trip_distance"] < 0)
        | (df["fare_amount"] < 0)
        | (df["total_amount"] < 0)
    )

    invalid_rows = int(invalid.sum())
    df = df.loc[~invalid].copy()

    # Remove exact duplicate records.
    duplicate_rows = int(df.duplicated().sum())
    df = df.drop_duplicates().copy()

    # Add useful columns for analytics.
    df["pickup_date"] = (
        df["tpep_pickup_datetime"].dt.date
    )

    df["trip_duration_minutes"] = (
        df["tpep_dropoff_datetime"]
        - df["tpep_pickup_datetime"]
    ).dt.total_seconds() / 60

    # Write cleaned data separately from the raw source.
    output_path.parent.mkdir(
        parents=True, exist_ok=True
    )
    df.to_parquet(output_path, index=False)

    report = {
        "source_rows": source_rows,
        "invalid_rows_removed": invalid_rows,
        "duplicate_rows_removed": duplicate_rows,
        "processed_rows": len(df),
        "output_path": str(output_path),
    }

    return report


if __name__ == "__main__":
    results = transform_data()

    print("Transformation completed:")
    for name, value in results.items():
        print(f"{name}: {value}")
