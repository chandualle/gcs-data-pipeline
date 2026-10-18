
cat > dags/nyc_taxi_pipeline.py <<'PY'
import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery, storage


PROJECT_ID = (
    os.environ.get("GCP_PROJECT")
    or os.environ.get("GOOGLE_CLOUD_PROJECT")
    or "qwiklabs-gcp-03-0ba9de6d12bc"
)

GCS_BUCKET = os.environ.get(
    "GCS_BUCKET",
    "qwiklabs-gcp-03-0ba9de6d12bc-taxi-pipeline-1792294179",
)

SOURCE_URL = (
    "https://d37ci6vzurychx.cloudfront.net/"
    "trip-data/yellow_tripdata_2024-01.parquet"
)

RAW_OBJECT = "raw/nyc_taxi/yellow_tripdata_2024-01.parquet"
PROCESSED_OBJECT = (
    "processed/nyc_taxi/yellow_tripdata_2024-01.parquet"
)

DATASET_ID = "nyc_taxi"
TABLE_ID = "yellow_trips"

REQUIRED_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "PULocationID",
    "DOLocationID",
]


def extract_to_gcs():
    """Download the source Parquet file and store it in GCS."""
    with tempfile.TemporaryDirectory() as temp_dir:
        local_path = Path(temp_dir) / "yellow_tripdata_2024-01.parquet"

        with requests.get(
            SOURCE_URL,
            stream=True,
            timeout=(15, 120),
        ) as response:
            response.raise_for_status()

            with local_path.open("wb") as output:
                for chunk in response.iter_content(
                    chunk_size=1024 * 1024
                ):
                    if chunk:
                        output.write(chunk)

        client = storage.Client(project=PROJECT_ID)
        client.bucket(GCS_BUCKET).blob(RAW_OBJECT).upload_from_filename(
            str(local_path)
        )

    print(f"Uploaded raw data to gs://{GCS_BUCKET}/{RAW_OBJECT}")


def validate_gcs_data():
    """Check the raw Parquet schema and basic data quality."""
    client = storage.Client(project=PROJECT_ID)

    with tempfile.TemporaryDirectory() as temp_dir:
        local_path = Path(temp_dir) / "raw.parquet"

        client.bucket(GCS_BUCKET).blob(RAW_OBJECT).download_to_filename(
            str(local_path)
        )

        df = pd.read_parquet(local_path)

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
            "rows": len(df),
            "columns": len(df.columns),
            "duplicate_rows": int(df.duplicated().sum()),
            "negative_fares": int((df["fare_amount"] < 0).sum()),
            "negative_totals": int((df["total_amount"] < 0).sum()),
        }

        print(f"Raw data validation report: {report}")

        if len(df) == 0:
            raise ValueError("The raw dataset is empty")


def transform_and_upload():
    """Clean raw data and upload the processed Parquet file to GCS."""
    client = storage.Client(project=PROJECT_ID)

    with tempfile.TemporaryDirectory() as temp_dir:
        raw_path = Path(temp_dir) / "raw.parquet"
        processed_path = Path(temp_dir) / "processed.parquet"

        client.bucket(GCS_BUCKET).blob(RAW_OBJECT).download_to_filename(
            str(raw_path)
        )

        df = pd.read_parquet(raw_path)
        source_rows = len(df)

        for column in [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
        ]:
            df[column] = pd.to_datetime(
                df[column], errors="coerce"
            )

        for column in [
            "trip_distance",
            "fare_amount",
            "total_amount",
        ]:
            df[column] = pd.to_numeric(
                df[column], errors="coerce"
            )

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

        duplicate_rows = int(df.duplicated().sum())
        df = df.drop_duplicates().copy()

        df["pickup_date"] = df["tpep_pickup_datetime"].dt.date

        df["trip_duration_minutes"] = (
            df["tpep_dropoff_datetime"]
            - df["tpep_pickup_datetime"]
        ).dt.total_seconds() / 60

        if df.empty:
            raise ValueError("No valid rows remain after transformation")

        df.to_parquet(processed_path, index=False)

        client.bucket(GCS_BUCKET).blob(
            PROCESSED_OBJECT
        ).upload_from_filename(str(processed_path))

        report = {
            "source_rows": source_rows,
            "invalid_rows_removed": invalid_rows,
            "duplicate_rows_removed": duplicate_rows,
            "processed_rows": len(df),
        }

        print(f"Transformation report: {report}")

        return int(len(df))


def load_to_bigquery():
    """Load the processed GCS Parquet object into BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)

    source_uri = f"gs://{GCS_BUCKET}/{PROCESSED_OBJECT}"
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    job = client.load_table_from_uri(
        source_uri,
        table_ref,
        job_config=job_config,
    )
    job.result()

    table = client.get_table(table_ref)
    print(f"Loaded {table.num_rows} rows into {table_ref}")

    return int(table.num_rows)


def verify_bigquery_count(**context):
    """Compare the transformed row count with the BigQuery table."""
    transformed_rows = context["ti"].xcom_pull(
        task_ids="transform_data"
    )
    loaded_rows = context["ti"].xcom_pull(
        task_ids="load_bigquery"
    )

    if transformed_rows != loaded_rows:
        raise ValueError(
            "Row count mismatch: "
            f"transformed={transformed_rows}, "
            f"loaded={loaded_rows}"
        )

    print(f"Verified {loaded_rows} rows in BigQuery.")


def check_bigquery_quality():
    """Check that the table is populated and has no negative fares."""
    client = bigquery.Client(project=PROJECT_ID)

    query = f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNTIF(fare_amount < 0) AS negative_fares
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    """

    result = list(client.query(query).result())[0]

    if result.total_rows == 0:
        raise ValueError("BigQuery table is empty")

    if result.negative_fares > 0:
        raise ValueError(
            f"Found {result.negative_fares} negative fares"
        )

    print(
        "Quality check passed: "
        f"{result.total_rows} rows, no negative fares."
    )


with DAG(
    dag_id="nyc_taxi_pipeline",
    description="Extract, validate, transform, and load NYC taxi data",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["nyc-taxi", "gcs", "bigquery"],
) as dag:

    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract_to_gcs,
    )

    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=validate_gcs_data,
    )

    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform_and_upload,
    )

    load_bigquery_task = PythonOperator(
        task_id="load_bigquery",
        python_callable=load_to_bigquery,
    )

    verify_data = PythonOperator(
        task_id="verify_data",
        python_callable=verify_bigquery_count,
    )

    quality_check = PythonOperator(
        task_id="check_bigquery_quality",
        python_callable=check_bigquery_quality,
    )

    (
        extract_data
        >> validate_data
        >> transform_data
        >> load_bigquery_task
        >> verify_data
        >> quality_check
    )
PY
