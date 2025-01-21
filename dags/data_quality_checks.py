from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryCreateEmptyDatasetOperator
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
quality_check_dataset_id = "data_quality_check"

# Define the path to the SQL files
SQL_PATH = '/usr/local/airflow/include/sql'

@dag(
    dag_id="data_quality_checks",
    default_args=default_args,
    schedule_interval=None,  # No schedule, manual trigger
    tags=["example"],
)
def data_quality_checks_pipeline():
    """
    A DAG to run data quality checks on tables in BigQuery based on CSV files already uploaded.
    """

    # Task to create the dataset if it doesn't exist
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=quality_check_dataset_id,
        gcp_conn_id=gcp_conn_id,
        location="US",
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

    # Dynamically map BigQueryCheckOperator tasks for each CSV file
    csv_files = process_files(list_files.output)

    def generate_check_sql(file_name):
        # Extract table name from the file name (assuming the file name corresponds to the table name)
        table_name = file_name.split('/')[-1].replace('.csv', '')
        
        # Fully qualify the table name with project and dataset
        fully_qualified_table_name = f"`{bq_project_id}.{dataset_id}.{table_name}`"
        
        # Load the SQL template
        with open(os.path.join(SQL_PATH, 'check_nulls.sql'), 'r') as sql_file:
            check_nulls_sql = sql_file.read()
        
        # Replace placeholders with actual values for table_name, project_id, and dataset_id
        check_nulls_sql = check_nulls_sql.replace('{table_name}', table_name)  # Replace alias in SELECT
        check_nulls_sql = check_nulls_sql.replace('{project_id}', bq_project_id)  # Replace project_id
        check_nulls_sql = check_nulls_sql.replace('{dataset_id}', dataset_id)  # Replace dataset_id
        check_nulls_sql = check_nulls_sql.replace('{project_id}.{dataset_id}.{table_name}', fully_qualified_table_name)  # Replace table reference in FROM

        return check_nulls_sql

    # Run the data quality check using BigQueryCheckOperator
    check_data_quality = BigQueryCheckOperator.partial(
        task_id="check_data_quality",
        use_legacy_sql=False,
        gcp_conn_id=gcp_conn_id,
    ).expand(
        sql=csv_files.map(generate_check_sql)
    )

    def generate_trans_dim_sql():
        """
        Load the trans_dim.sql file and replace placeholders with actual values.
        """
        table_name = "Trans_dim"
        fully_qualified_table_name = f"`{bq_project_id}.{dataset_id}.{table_name}`"
        
        # Load the SQL template
        with open(os.path.join(SQL_PATH, 'schema/trans_dim.sql'), 'r') as sql_file:
            trans_dim_sql = sql_file.read()
        
        # Replace placeholders with actual values for table_name, project_id, and dataset_id
        trans_dim_sql = trans_dim_sql.replace('{table_name}', table_name)  # Replace alias in SELECT
        trans_dim_sql = trans_dim_sql.replace('{project_id}', bq_project_id)  # Replace project_id
        trans_dim_sql = trans_dim_sql.replace('{dataset_id}', dataset_id)  # Replace dataset_id
        trans_dim_sql = trans_dim_sql.replace('{project_id}.{dataset_id}.{table_name}', fully_qualified_table_name)  # Replace table reference in FROM

        return trans_dim_sql

    # Task to run the trans_dim.sql script
    run_trans_dim_sql = BigQueryCheckOperator(
        task_id="run_trans_dim_sql",
        sql=generate_trans_dim_sql(),
        use_legacy_sql=False,
        gcp_conn_id=gcp_conn_id,
    )

    # Define task dependencies
    create_dataset >> list_files >> csv_files >> check_data_quality >> run_trans_dim_sql


# Instantiate the DAG
data_quality_checks_dag = data_quality_checks_pipeline()
