import os

PROJECT_ID = os.environ.get(
    "GCP_PROJECT",
    "qwiklabs-gcp-04-3b4814bb5e4a",
)

SOURCE_URL = (
    "https://d37ci6vzurychx.cloudfront.net/"
    "trip-data/yellow_tripdata_2024-01.parquet"
)

RAW_DATA_PATH = "data/raw/yellow_tripdata_2024-01.parquet"
PROCESSED_DATA_PATH = "data/processed/yellow_tripdata_2024-01.parquet"

GCS_RAW_PREFIX = "raw/nyc_taxi/"
GCS_PROCESSED_PREFIX = "processed/nyc_taxi/"

BIGQUERY_DATASET = "nyc_taxi"
BIGQUERY_TABLE = "yellow_trips"