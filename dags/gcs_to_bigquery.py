from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
)
from datetime import datetime
import logging
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define default arguments
default_args = {
    "start_date": datetime(2023, 1, 1),
    "catchup": False,
}

bucket_name = os.getenv("BUCKET_NAME")
gcp_conn_id = os.getenv("GCP_CONN_ID")
dataset_id = "ecommerce"
bq_project_id = os.getenv("BQ_PROJECT_ID")


@dag(
    dag_id="gcs_to_bigquery",
    default_args=default_args,
    schedule_interval=None,  # No schedule, manual trigger
    tags=["example"],
)
def gcs_to_bigquery_pipeline():
    """
    A DAG to load CSV files from GCS to BigQuery with schema autodetection.
    """

    # Task to create the BigQuery dataset (runs only once at the start)
    create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_retail_dataset",
        dataset_id=dataset_id,
        gcp_conn_id=gcp_conn_id,
    )
    logger.info(
        f"BigQuery dataset creation task initialized for dataset ID: {dataset_id}"
    )

    # Task to list all objects in the GCS bucket under the 'raw/' folder (runs only once)
    list_files = GCSListObjectsOperator(
        task_id="list_files",
        bucket=bucket_name,
        prefix="raw/",  # List files in the 'raw/' folder
        gcp_conn_id=gcp_conn_id,
    )
    logger.info("Listing files in the bucket...")

    # Task to process the list of files
    @task
    def process_files(file_list):
        """
        Filter the list of files to include only CSV files.
        """
        logger.info(f"Raw file list from GCS: {file_list}")
        csv_files = [file for file in file_list if file.endswith(".csv")]
        logger.info(f"Filtered CSV files: {csv_files}")
        return csv_files

    # Dynamically map GCSToBigQueryOperator tasks for each CSV file
    csv_files = process_files(list_files.output)

    load_to_bigquery = GCSToBigQueryOperator.partial(
        task_id="load_to_bigquery",
        gcp_conn_id=gcp_conn_id,  # Google Cloud connection ID
        bucket=bucket_name,  # GCS bucket name
        source_format="CSV",  # Source file format
        skip_leading_rows=1,  # Skip the header row
        write_disposition="WRITE_TRUNCATE",  # Overwrite the table if it exists
        create_disposition="CREATE_IF_NEEDED",  # Create the table if it doesn't exist
        autodetect=True,  # Enable schema autodetection
        max_bad_records=10,  # Allow up to 10 bad records before failing
        ignore_unknown_values=True,  # Ignore unknown values in the CSV file
        location="US",  # BigQuery dataset location
    ).expand(
        source_objects=csv_files.map(lambda file_name: [file_name]),
        destination_project_dataset_table=csv_files.map(
            lambda file_name: f"{bq_project_id}.{dataset_id}.{file_name.split('/')[-1].replace('.csv', '')}"
        ),
    )

    # Define task dependencies
    create_retail_dataset >> list_files >> csv_files >> load_to_bigquery


# Instantiate the DAG
gcs_to_bigquery_dag = gcs_to_bigquery_pipeline()
