from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
)
from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import logging
import os
import importlib
import sys
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

gcp_conn_id = os.getenv("GCP_CONN_ID")
source_dataset_id = "ecommerce"
target_dataset_id = "ecommerce_transformed"
bq_project_id = os.getenv("BQ_PROJECT_ID")

# Mapping of table names to transformation scripts
table_to_transformation_script = {
    "fact_table": "fact_table",
    "customer_dim": "customer_dim",
    "item_dim": "item_dim",
    "Trans_dim": "Trans_dim",
    "time_dim": "time_dim",
    "store_dim": "store_dim",
}


@dag(
    dag_id="feature_engineering",
    default_args=default_args,
    schedule_interval=None,  # No schedule, manual trigger
    tags=["example"],
)
def feature_engineering_pipeline():
    """
    A DAG to perform feature engineering on BigQuery tables.
    """

    # Task to create the new BigQuery dataset for transformed tables (runs only once at the start)
    create_transformed_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_transformed_dataset",
        dataset_id=target_dataset_id,
        gcp_conn_id=gcp_conn_id,
    )
    logger.info(
        f"BigQuery dataset creation task initialized for dataset ID: {target_dataset_id}"
    )

    # Task to perform feature engineering
    @task
    def feature_engineering():
        """
        Perform feature engineering on the BigQuery tables.
        """
        client = bigquery.Client(project=bq_project_id)

        # Add the include folder to the Python path
        sys.path.append(os.path.join(os.path.dirname(__file__), "../include"))

        for table_name, module_name in table_to_transformation_script.items():
            # Read data from BigQuery
            query = f"SELECT * FROM `{bq_project_id}.{source_dataset_id}.{table_name}`"
            query_job = client.query(query)
            results = query_job.result()
            df = results.to_dataframe()

            if df is None or df.empty:
                logger.warning(f"No data found for table {table_name}")
                continue

            # Log the first few rows of the DataFrame
            logger.info(f"First few rows of {table_name}:\n{df.head()}")

            # Dynamically import the transformation module
            try:
                transform_module = importlib.import_module(
                    f"transformations.{module_name}"
                )
            except ImportError as e:
                logger.error(f"Error importing module {module_name}: {e}")
                continue

            # Perform the transformation
            try:
                transformed_df = transform_module.transform(df)
            except Exception as e:
                logger.error(f"Error transforming data for table {table_name}: {e}")
                continue

            if transformed_df is None or transformed_df.empty:
                logger.warning(f"Transformed data for table {table_name} is empty")
                continue

            # Write the transformed data back to BigQuery
            try:
                transformed_df.to_gbq(
                    destination_table=f"{target_dataset_id}.{table_name}_transformed",
                    project_id=bq_project_id,
                    if_exists="replace",
                )
                logger.info(
                    f"Feature engineering completed for {table_name} and data uploaded to BigQuery"
                )
            except Exception as e:
                logger.error(
                    f"Error uploading transformed data for table {table_name} to BigQuery: {e}"
                )

    # Define the DAG structure
    create_transformed_dataset >> feature_engineering()


# Instantiate the DAG
feature_engineering_and_eda_dag = feature_engineering_pipeline()
