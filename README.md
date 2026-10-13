# GCP Data Engineering Pipeline

A hands-on data engineering project built using Google Cloud Platform.

## Objective

Build a repeatable ETL pipeline that extracts sample source data, validates and transforms it using Python, stores raw and processed data in Google Cloud Storage, and loads analytical tables into BigQuery.

The pipeline will eventually be orchestrated using Apache Airflow.

## Technology Stack

- Python
- Google Cloud Storage
- BigQuery
- Google Cloud CLI
- Git
- Apache Airflow / Cloud Composer

## Project Structure

- `src/` — ETL and data quality logic
- `sql/` — BigQuery DDL and analytical SQL
- `tests/` — Automated tests
- `config/` — Non-secret configuration
- `dags/` — Airflow DAG definitions

## Development Status

Initial repository setup.