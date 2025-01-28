from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from google.cloud import storage
import pandas as pd
from datetime import datetime
import logging
import os
from dotenv import load_dotenv
import io

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

    # Task to upload a single file to BigQuery using Pandas
    @task
    def upload_to_bigquery(file_name):
        """
        Upload a single file to BigQuery using Pandas.
        """
        table_name = file_to_table_mapping.get(file_name.split('/')[-1], 'unknown_table')
        
        # Initialize GCS client
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(f"raw/{file_name}")
        
        # Download the CSV file content
        content = blob.download_as_text(encoding='ISO-8859-1')
        
        # Read the CSV file into a Pandas DataFrame
        df = pd.read_csv(io.StringIO(content))
        
        # Upload the DataFrame to BigQuery
        df.to_gbq(
            destination_table=f"{dataset_id}.{table_name}",
            project_id=bq_project_id,
            if_exists="replace",
            table_schema=None,  # Automatically infer schema
        )
        logger.info(f"Uploaded {file_name} to BigQuery table {table_name}")

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
