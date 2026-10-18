
import os

from google.cloud import storage

from src.extract import download_source
from src.validate import validate_source
from src.transform import transform_data
from src.load_gcs import upload_file
from src.load_bigquery import load_to_bigquery

from config.pipeline_config import (
    RAW_DATA_PATH,
    PROCESSED_DATA_PATH,
)


def run_pipeline():
    bucket_name = os.environ.get("GCS_BUCKET")

    if not bucket_name:
        raise ValueError("Set the GCS_BUCKET environment variable")

    print("\nSTEP 1: Extract source data")
    download_source()

    print("\nSTEP 2: Validate raw data")
    validation_report = validate_source(RAW_DATA_PATH)
    print(validation_report)

    print("\nSTEP 3: Transform source data")
    transformation_report = transform_data()
    print(transformation_report)

    print("\nSTEP 4: Upload raw and processed data to GCS")
    client = storage.Client()
    upload_file(
        client,
        bucket_name,
        RAW_DATA_PATH,
        "raw/nyc_taxi/yellow_tripdata_2024-01.parquet",
    )
    upload_file(
        client,
        bucket_name,
        PROCESSED_DATA_PATH,
        "processed/nyc_taxi/yellow_tripdata_2024-01.parquet",
    )

    print("\nSTEP 5: Load processed data into BigQuery")
    load_report = load_to_bigquery()

    expected_rows = transformation_report["processed_rows"]
    actual_rows = load_report["rows_loaded"]

    print("\nSTEP 6: Verify loaded row count")
    if expected_rows != actual_rows:
        raise RuntimeError(
            f"Row count mismatch: processed={expected_rows}, "
            f"BigQuery={actual_rows}"
        )

    print(f"Verified {actual_rows} rows in BigQuery.")
    print("\nPIPELINE COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    run_pipeline()
