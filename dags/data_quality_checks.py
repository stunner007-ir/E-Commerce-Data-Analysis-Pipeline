from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import os
import io
import logging

# Fetch environment variables
BUCKET_NAME = os.getenv("BUCKET_NAME")  # GCS bucket name
GCP_CONN_ID = os.getenv("GCP_CONN_ID")  # Airflow GCP connection ID
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")  # BigQuery project ID
BQ_DATASET_NAME = "ecommerce"  # BigQuery dataset name
BQ_TABLE_NAME = "csv_row_count_table"  # BigQuery table name

# Define the schema
SCHEMA_FIELDS = [
    {'name': 'table_name', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'source_rc', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    {'name': 'target_rc', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    {'name': 'date', 'type': 'DATE', 'mode': 'REQUIRED'},
    {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
    {'name': 'difference', 'type': 'INTEGER', 'mode': 'REQUIRED'},
]

# Define the DAG's default arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': datetime(2025, 1, 1),
}

with DAG(
    'process_csv_to_bq',
    default_args=default_args,
    description='DAG to process CSV files from GCS and store row count in BigQuery',
    schedule_interval=None,  # Change as needed (e.g., '0 0 * * *' for daily)
    catchup=False,
) as dag:

    # GCS path to the raw folder
    gcs_path = "gs://ecommerce_data_stunner007/raw/"

    # Task to create the BigQuery table if it doesn't exist
    def create_bq_table():
        logging.info("Creating BigQuery table if it doesn't exist")
        client = bigquery.Client(project=BQ_PROJECT_ID)
        dataset_ref = client.dataset(BQ_DATASET_NAME)
        table_ref = dataset_ref.table(BQ_TABLE_NAME)
        
        schema = [bigquery.SchemaField(field['name'], field['type'], mode=field['mode']) for field in SCHEMA_FIELDS]
        
        table = bigquery.Table(table_ref, schema=schema)
        try:
            client.get_table(table_ref)
            logging.info("Table already exists")
        except Exception:
            client.create_table(table)
            logging.info("Table created")

    create_table_task = PythonOperator(
        task_id='create_table_task',
        python_callable=create_bq_table,
    )

    # Task to list all CSV files in the GCS path
    def list_csv_files():
        logging.info("Pass 1: Listing CSV files")
        client = storage.Client()
        bucket = client.get_bucket(BUCKET_NAME)
        blobs = bucket.list_blobs(prefix=gcs_path.replace("gs://", "").replace(BUCKET_NAME + "/", ""))
        csv_files = [blob.name for blob in blobs if blob.name.endswith('.csv')]
        logging.info(f"Pass 1: Found {len(csv_files)} CSV files")
        return csv_files

    list_files_task = PythonOperator(
        task_id='list_files_task',
        python_callable=list_csv_files,
    )

    # Function to get row count from BigQuery
    def get_bq_row_count(table_name):
        client = bigquery.Client(project=BQ_PROJECT_ID)
        query = f"SELECT COUNT(*) as row_count FROM `{BQ_PROJECT_ID}.{BQ_DATASET_NAME}.{table_name}`"
        query_job = client.query(query)
        results = query_job.result()
        for row in results:
            return row.row_count

    # Task to process each CSV file
    def process_csv_file(blob_name):
        logging.info(f"Pass 2: Processing file {blob_name}")
        client = storage.Client()
        bucket = client.get_bucket(BUCKET_NAME)
        blob = bucket.blob(blob_name)
        
        # Read the file into a pandas DataFrame
        file_content = blob.download_as_text(encoding='ISO-8859-1')
        df = pd.read_csv(io.StringIO(file_content))
        
        # Row count calculations
        source_rc = len(df)  # Number of rows in the CSV file
        table_name = blob_name.split('/')[-1].replace('.csv', '')  # Extract table name from file name
        target_rc = get_bq_row_count(table_name)  # Get row count from BigQuery
        difference = source_rc - target_rc
        
        # Get current date and timestamp
        current_date = datetime.now().date()
        current_timestamp = datetime.now()
        
        # Prepare data for BigQuery insert
        data = {
            'table_name': blob_name.split('/')[-1],  # Extract file name from path
            'source_rc': source_rc,
            'target_rc': target_rc,
            'date': current_date.isoformat(),  # Convert date to string
            'timestamp': current_timestamp.isoformat(),  # Convert timestamp to string
            'difference': difference,
        }

        logging.info(f"Pass 2: Processed file {blob_name} with {source_rc} rows and target {target_rc} rows")
        return data

    def process_files(**context):
        logging.info("Pass 3: Processing all files")
        csv_files = context['task_instance'].xcom_pull(task_ids='list_files_task')
        for csv_file in csv_files:
            row_count_data = process_csv_file(csv_file)
            context['task_instance'].xcom_push(key=csv_file, value=row_count_data)
        logging.info("Pass 3: Completed processing all files")

    process_files_task = PythonOperator(
        task_id='process_files_task',
        python_callable=process_files,
        provide_context=True,
    )

    # Task to insert the row count data into BigQuery
    def insert_to_bigquery(**context):
        logging.info("Pass 4: Inserting data into BigQuery")
        csv_files = context['task_instance'].xcom_pull(task_ids='list_files_task')
        for csv_file in csv_files:
            row_count_data = context['task_instance'].xcom_pull(task_ids='process_files_task', key=csv_file)
            if row_count_data:
                logging.info(f"Pass 4: Inserting data for file {csv_file}")
                client = bigquery.Client(project=BQ_PROJECT_ID)
                table_ref = client.dataset(BQ_DATASET_NAME).table(BQ_TABLE_NAME)
                errors = client.insert_rows_json(table_ref, [row_count_data])  # Insert a single row
                if errors:
                    logging.error(f"Pass 4: Error inserting rows into BigQuery: {errors}")
                    raise Exception(f"Error inserting rows into BigQuery: {errors}")
                logging.info(f"Pass 4: Successfully inserted data for file {csv_file}")
        logging.info("Pass 4: Completed inserting data into BigQuery")

    insert_to_bq_task = PythonOperator(
        task_id='insert_to_bq_task',
        python_callable=insert_to_bigquery,
        provide_context=True,
    )

    # Define the task flow
    create_table_task >> list_files_task >> process_files_task >> insert_to_bq_task
