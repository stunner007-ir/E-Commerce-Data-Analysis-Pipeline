from airflow.decorators import dag, task
from datetime import datetime
import os
import logging
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dag(
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Use schedule_interval instead of schedule
    catchup=False,
    tags=["retail"],
    dag_id="local_to_gcs",
)
def retail():
    dataset_folder = "include/dataset/"
    bucket_name = os.getenv("BUCKET_NAME")
    gcp_conn_id = os.getenv("GCP_CONN_ID")

    # List all CSV files in the dataset folder
    csv_files = [f for f in os.listdir(dataset_folder) if f.endswith(".csv")]

    @task
    def upload_to_gcs(src_path, dst_path, task_id):
        try:
            LocalFilesystemToGCSOperator(
                task_id=task_id,
                src=src_path,
                dst=dst_path,
                bucket=bucket_name,
                gcp_conn_id=gcp_conn_id,
                mime_type="text/csv",
            ).execute(context={})
            logger.info(
                f"Successfully uploaded {src_path} to {dst_path} in bucket {bucket_name}"
            )
        except Exception as e:
            logger.error(
                f"Failed to upload {src_path} to {dst_path} in bucket {bucket_name}: {e}"
            )

    # Create a list to hold all upload tasks
    upload_tasks = []

    for csv_file in csv_files:
        src_path = os.path.join(dataset_folder, csv_file)
        dst_path = f"raw/{csv_file}"
        task_id = f'upload_{os.path.basename(src_path).split(".")[0]}_to_gcs'
        upload_task = upload_to_gcs(src_path, dst_path, task_id)
        upload_tasks.append(upload_task)

    # Set task dependencies (if any)
    # In this case, there are no dependencies to set since we removed the create_retail_dataset task


retail_dag = retail()  # Instantiate the DAG
