
import os
from pathlib import Path

from google.cloud import storage

from config.pipeline_config import (
    PROCESSED_DATA_PATH,
    RAW_DATA_PATH,
)


def upload_file(
    client: storage.Client,
    bucket_name: str,
    local_path: str,
    destination: str,
) -> None:
    """Upload one local file to a GCS object."""
    path = Path(local_path)

    if not path.is_file():
        raise FileNotFoundError(f"File not found: {path}")

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination)
    blob.upload_from_filename(str(path))

    print(f"Uploaded {path} -> gs://{bucket_name}/{destination}")


def main() -> None:
    bucket_name = os.environ.get("GCS_BUCKET")

    if not bucket_name:
        raise ValueError("Set the GCS_BUCKET environment variable")

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


if __name__ == "__main__":
    main()
