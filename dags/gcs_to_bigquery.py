from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
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
    "start_date": datetime(2025, 1, 1),
    "catchup": False,
}

bucket_name = os.getenv("BUCKET_NAME")
gcp_conn_id = os.getenv("GCP_CONN_ID")
dataset_id = "ecommerce"
bq_project_id = os.getenv("BQ_PROJECT_ID")

# Mapping of file names to table names
file_to_table_mapping = {
    "Trans_dim.csv": "Trans_dim",
    "customer_dim.csv": "customer_dim",
    "fact_table.csv": "fact_table",
    "item_dim.csv": "item_dim",
    "store_dim.csv": "store_dim",
    "time_dim.csv": "time_dim",
}

@dag(
    dag_id="gcs_to_bigquery_sequential",
    default_args=default_args,
    schedule_interval=None,  # No schedule, manual trigger
    tags=["example"],
)
def gcs_to_bigquery_pipeline_sequential():
    """
    A DAG to load CSV files from GCS to BigQuery sequentially.
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

    # Task to upload a single file to BigQuery
    def upload_to_bigquery(file_name):
        """
        Upload a single file to BigQuery.
        """
        table_name = file_to_table_mapping.get(file_name.split('/')[-1], 'unknown_table')
        return GCSToBigQueryOperator(
            task_id=f"load_{table_name}",
            gcp_conn_id=gcp_conn_id,
            bucket=bucket_name,
            source_objects=[f"raw/{file_name}"],
            destination_project_dataset_table=f"{bq_project_id}.{dataset_id}.{table_name}",
            source_format="csv",
            skip_leading_rows=1,
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            max_bad_records=10,
            autodetect=True,
            ignore_unknown_values=True,
            location="US",
        )

    # Define the DAG structure
    create_retail_dataset >> list_files

    # Process the list of files
    csv_files = process_files(list_files.output)
    previous_task = csv_files

    # Sequentially create upload tasks for each file
    for file_name in file_to_table_mapping.keys():
        upload_task = upload_to_bigquery(file_name)
        previous_task >> upload_task
        previous_task = upload_task


# Instantiate the DAG
gcs_to_bigquery_dag_sequential = gcs_to_bigquery_pipeline_sequential()
