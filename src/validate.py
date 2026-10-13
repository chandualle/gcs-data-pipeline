
from pathlib import Path

import pandas as pd

REQUIRED_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "PULocationID",
    "DOLocationID",
]


def validate_source(file_path: str) -> dict:
    """Check source schema and basic data quality."""
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"Source file not found: {path}")

    df = pd.read_parquet(path)

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Required columns are missing: {missing_columns}"
        )

    report = {
        "file": str(path),
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "missing_values": (
            df[REQUIRED_COLUMNS].isna().sum().to_dict()
        ),
        "duplicate_rows": int(df.duplicated().sum()),
        "negative_trip_distance": int(
            (df["trip_distance"] < 0).sum()
        ),
        "negative_fare_amount": int(
            (df["fare_amount"] < 0).sum()
        ),
        "negative_total_amount": int(
            (df["total_amount"] < 0).sum()
        ),
    }

    return report


if __name__ == "__main__":
    source = "data/raw/yellow_tripdata_2024-01.parquet"
    results = validate_source(source)

    for name, value in results.items():
        print(f"{name}: {value}")
