from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from google.cloud import storage, bigquery
import pandas as pd
from datetime import datetime
import os
import io
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Fetch environment variables
BUCKET_NAME = os.getenv("BUCKET_NAME")
GCP_CONN_ID = os.getenv("GCP_CONN_ID")
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET_NAME = "ecommerce"
BQ_QUALITY_DATASET_NAME = "data_quality_results"
BQ_TABLE_NAME = "csv_row_count_table"

# Define the schema for the BigQuery table
SCHEMA_FIELDS = [
    {"name": "table_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "source_rc", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "target_rc", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "difference", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "status", "type": "STRING", "mode": "REQUIRED"},
]

# Define the DAG's default arguments
default_args = {
    "owner": "airflow",
    "retries": 1,
    "start_date": datetime(2025, 1, 1),
}

# Initialize the DAG
with DAG(
    "ecommerce_row_count_check",
    default_args=default_args,
    description="DAG to process CSV files from GCS and store row count in BigQuery",
    schedule_interval=None,
    catchup=False,
) as dag:

    gcs_path = "gs://ecommerce_data_stunner007/raw/"

    def create_bq_dataset():
        """Create BigQuery dataset if it does not exist."""
        client = bigquery.Client(project=BQ_PROJECT_ID)
        dataset_ref = client.dataset(BQ_QUALITY_DATASET_NAME)
        try:
            client.get_dataset(dataset_ref)
            logger.info(f"Dataset {BQ_QUALITY_DATASET_NAME} already exists.")
        except Exception:
            client.create_dataset(dataset_ref)
            logger.info(f"Dataset {BQ_QUALITY_DATASET_NAME} created.")

    create_dataset_task = PythonOperator(
        task_id="create_data_quality_results_dataset",
        python_callable=create_bq_dataset,
    )

    def create_bq_table():
        """Create BigQuery table with defined schema if it does not exist."""
        client = bigquery.Client(project=BQ_PROJECT_ID)
        dataset_ref = client.dataset(BQ_QUALITY_DATASET_NAME)
        table_ref = dataset_ref.table(BQ_TABLE_NAME)

        schema = [bigquery.SchemaField(field["name"], field["type"], mode=field["mode"]) for field in SCHEMA_FIELDS]
        table = bigquery.Table(table_ref, schema=schema)

        try:
            client.get_table(table_ref)
            logger.info("Table already exists.")
        except Exception:
            client.create_table(table)
            logger.info("Table created.")

    create_table_task = PythonOperator(
        task_id="create_table_task",
        python_callable=create_bq_table,
    )

    def list_csv_files():
        """List all CSV files in the specified GCS path."""
        client = storage.Client()
        bucket = client.get_bucket(BUCKET_NAME)
        blobs = bucket.list_blobs(prefix=gcs_path.replace("gs://", "").replace(BUCKET_NAME + "/", ""))
        return [blob.name for blob in blobs if blob.name.endswith(".csv")]

    list_files_task = PythonOperator(
        task_id="list_files_task",
        python_callable=list_csv_files,
    )

    def get_bq_row_count(table_name):
        """Get the row count of a specified BigQuery table."""
        client = bigquery.Client(project=BQ_PROJECT_ID)
        query = f"SELECT COUNT(*) as row_count FROM `{BQ_PROJECT_ID}.{BQ_DATASET_NAME}.{table_name}`"
        query_job = client.query(query)
        results = query_job.result()
        return next(results).row_count

    def process_csv_file(blob_name):
        """Process a CSV file from GCS and return row count data."""
        client = storage.Client()
        bucket = client.get_bucket(BUCKET_NAME)
        blob = bucket.blob(blob_name)

        file_content = blob.download_as_text(encoding="ISO-8859-1")
        df = pd.read_csv(io.StringIO(file_content))

        source_rc = len(df)
        table_name = blob_name.split("/")[-1].replace(".csv", "")
        target_rc = get_bq_row_count(table_name)
        difference = source_rc - target_rc
        status = "pass" if difference == 0 else "fail"

        current_date = datetime.now().date()
        current_timestamp = datetime.now()

        return {
            "table_name": table_name,
            "source_rc": source_rc,
            "target_rc": target_rc,
            "date": current_date.isoformat(),
            "timestamp": current_timestamp.isoformat(),
            "difference": difference,
            "status": status,
        }

    def process_files(**context):
        """Process all CSV files and push row count data to XCom."""
        csv_files = context["task_instance"].xcom_pull(task_ids="list_files_task")
        for csv_file in csv_files:
            row_count_data = process_csv_file(csv_file)
            context["task_instance"].xcom_push(key=csv_file, value=row_count_data)

    process_files_task = PythonOperator(
        task_id="process_files_task",
        python_callable=process_files,
        provide_context=True,
    )

    def insert_to_bigquery(**context):
        """Insert row count data into BigQuery."""
        csv_files = context["task_instance"].xcom_pull(task_ids="list_files_task")
        for csv_file in csv_files:
            row_count_data = context["task_instance"].xcom_pull(task_ids="process_files_task", key=csv_file)
            if row_count_data:
                client = bigquery.Client(project=BQ_PROJECT_ID)
                table_ref = client.dataset(BQ_QUALITY_DATASET_NAME).table(BQ_TABLE_NAME)
                errors = client.insert_rows_json(table_ref, [row_count_data])
                if errors:
                    logger.error(f"Error inserting rows into BigQuery: {errors}")
                    raise Exception(f"Error inserting rows into BigQuery: {errors}")

    insert_to_bq_task = PythonOperator(
        task_id="insert_to_bq_task",
        python_callable=insert_to_bigquery,
        provide_context=True,
    )

    # Define task dependencies
    (
        create_dataset_task
        >> create_table_task
        >> list_files_task
        >> process_files_task
        >> insert_to_bq_task
    )
