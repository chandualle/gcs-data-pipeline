
import os

from google.cloud import bigquery


def load_to_bigquery() -> dict:
    """Load processed Parquet data from GCS into BigQuery."""
    project_id = os.environ.get("GCP_PROJECT")
    bucket_name = os.environ.get("GCS_BUCKET")

    if not project_id:
        raise ValueError("Set the GCP_PROJECT environment variable")

    if not bucket_name:
        raise ValueError("Set the GCS_BUCKET environment variable")

    dataset_id = "nyc_taxi"
    table_id = "yellow_trips"

    source_uri = (
        f"gs://{bucket_name}/processed/nyc_taxi/"
        "yellow_tripdata_2024-01.parquet"
    )

    client = bigquery.Client(project=project_id)

    table_ref = f"{project_id}.{dataset_id}.{table_id}"

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

    result = {
        "source_uri": source_uri,
        "destination_table": table_ref,
        "rows_loaded": table.num_rows,
    }

    print("BigQuery load completed:")
    for key, value in result.items():
        print(f"{key}: {value}")

    return result


if __name__ == "__main__":
    load_to_bigquery()
