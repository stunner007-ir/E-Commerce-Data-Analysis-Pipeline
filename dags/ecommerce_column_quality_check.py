import os
import logging
from datetime import datetime
import pandas as pd
from pandas.api.types import is_numeric_dtype
from google.cloud import storage, bigquery
from airflow import DAG
from airflow.operators.python import PythonOperator
import io

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variables
BUCKET_NAME = os.getenv("BUCKET_NAME")
GCP_CONN_ID = os.getenv("GCP_CONN_ID")
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET_NAME = "ecommerce"
BQ_TABLE_NAME = "column_check_table"  # Target table for quality results
BQ_QUALITY_DATASET_NAME = "data_quality_results"
SOURCE_PREFIX = "raw/"

# Schema for the quality results table
QUALITY_TABLE_SCHEMA = [
    bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("column_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("src", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("tgt", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("diff", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("ts", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
]

def digital_root(n: int) -> int:
    """Calculate the digital root of a number."""
    while n > 9:
        n = sum(int(digit) for digit in str(n))
    return n

def sum_digits_once(n: int) -> int:
    """Sum the digits of a number once."""
    return sum(int(digit) for digit in str(n))

def create_quality_table():
    """Create the quality results table if it does not exist or if its schema is outdated."""
    client = bigquery.Client(project=BQ_PROJECT_ID)
    table_id = f"{BQ_PROJECT_ID}.{BQ_QUALITY_DATASET_NAME}.{BQ_TABLE_NAME}"
    dataset_ref = client.dataset(BQ_QUALITY_DATASET_NAME)
    table_ref = dataset_ref.table(BQ_TABLE_NAME)

    try:
        table = client.get_table(table_ref)
        field_names = [field.name for field in table.schema]
        if "diff" not in field_names:
            logger.info(f"Table {table_id} does not have 'diff' field. Dropping and recreating...")
            client.delete_table(table_ref)
            table = bigquery.Table(table_ref, schema=QUALITY_TABLE_SCHEMA)
            client.create_table(table)
            logger.info(f"Re-created table {table_id} with updated schema.")
        else:
            logger.info(f"Quality table {table_id} already exists with the correct schema.")
    except Exception as e:
        logger.error(f"Error creating quality table {table_id}: {e}")
        try:
            table = bigquery.Table(table_ref, schema=QUALITY_TABLE_SCHEMA)
            client.create_table(table)
            logger.info(f"Created quality table: {table_id}")
        except Exception as e:
            logger.error(f"Failed to create quality table {table_id}: {e}")

def task_list_files(**context):
    """List CSV files from GCS under the specified prefix."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        files = [blob.name for blob in bucket.list_blobs(prefix=SOURCE_PREFIX) if blob.name.endswith(".csv")]
        logger.info(f"Found {len(files)} files in GCS with prefix {SOURCE_PREFIX}")
        return files
    except Exception as e:
        logger.error(f"Error listing files in GCS: {e}")
        return []

def task_list_bq_tables(**context):
    """List table names in the source BigQuery dataset."""
    try:
        client = bigquery.Client(project=BQ_PROJECT_ID)
        dataset_ref = client.dataset(BQ_DATASET_NAME)
        tables = client.list_tables(dataset_ref)
        table_list = [table.table_id for table in tables]
        logger.info(f"Retrieved {len(table_list)} tables from BigQuery dataset {BQ_DATASET_NAME}")
        return table_list
    except Exception as e:
        logger.error(f"Error listing tables in BigQuery dataset {BQ_DATASET_NAME}: {e}")
        return []

def get_gcs_file_data(file_path: str) -> pd.DataFrame:
    """Download a CSV file from GCS and return it as a DataFrame."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(file_path)
        content = blob.download_as_text(encoding="ISO-8859-1")
        logger.info(f"Downloaded file: {file_path} from GCS")
        return pd.read_csv(io.StringIO(content))
    except Exception as e:
        logger.error(f"Error downloading file {file_path} from GCS: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error

def get_bq_table_data(table_name: str, dataset: str) -> pd.DataFrame:
    """Query a BigQuery table and return its data as a DataFrame."""
    try:
        client = bigquery.Client(project=BQ_PROJECT_ID)
        table_id = f"{BQ_PROJECT_ID}.{dataset}.{table_name}"
        query = f"SELECT * FROM `{table_id}`"
        logger.info(f"Querying BigQuery table: {table_id}")
        return client.query(query).to_dataframe()
    except Exception as e:
        logger.error(f"Error querying BigQuery table {table_name}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error

def task_perform_checks(**context):
    """
    Perform column checks by comparing source CSV and target BigQuery tables.
    For numeric columns, sum values and apply digit sum if exceeding threshold.
    For non-numeric columns, calculate total string length.
    """
    ti = context["ti"]
    files = ti.xcom_pull(task_ids="list_files")
    bq_tables = ti.xcom_pull(task_ids="list_bq_tables")
    results = []
    timestamp = datetime.utcnow().isoformat()
    THRESHOLD = 10**6

    if not files:
        logger.warning("No files found in GCS.")
    if not bq_tables:
        logger.warning("No tables found in BigQuery dataset.")

    for file_path in files:
        base_name = os.path.basename(file_path)
        table_name = os.path.splitext(base_name)[0]
        if table_name not in bq_tables:
            logger.warning(f"BigQuery table {table_name} not found. Skipping file {file_path}.")
            continue

        source_df = get_gcs_file_data(file_path)
        target_df = get_bq_table_data(table_name, BQ_DATASET_NAME)

        if source_df.empty or target_df.empty:
            logger.warning(f"Skipping checks for {file_path} due to empty DataFrame.")
            continue

        common_cols = set(source_df.columns).intersection(set(target_df.columns))
        logger.info(f"Performing column checks for table {table_name} on columns: {common_cols}")

        for col in common_cols:
            try:
                if is_numeric_dtype(source_df[col]):
                    source_total = source_df[col].sum()
                    target_total = target_df[col].sum()
                    if source_total > THRESHOLD:
                        prev = source_total
                        source_total = sum_digits_once(int(source_total))
                        logger.info(f"Column {col} in table {table_name}: source sum {prev} exceeded {THRESHOLD}, digit sum applied -> {source_total}")
                    if target_total > THRESHOLD:
                        prev = target_total
                        target_total = sum_digits_once(int(target_total))
                        logger.info(f"Column {col} in table {table_name}: target sum {prev} exceeded {THRESHOLD}, digit sum applied -> {target_total}")
                else:
                    source_total = int(source_df[col].astype(str).apply(len).sum())
                    target_total = int(target_df[col].astype(str).apply(len).sum())

                diff = source_total - target_total
                status = "PASS" if source_total == target_total else "FAIL"
                logger.info(f"Table: {table_name}, Column: {col}, Src: {source_total}, Tgt: {target_total}, Diff: {diff}, Status: {status}")
                results.append({
                    "table_name": table_name,
                    "column_name": col,
                    "source": source_total,
                    "target": target_total,
                    "diff": diff,
                    "timestamp": timestamp,
                    "status": status
                })
            except Exception as e:
                logger.error(f"Error processing column {col} in table {table_name}: {e}")

    ti.xcom_push(key="results", value=results)

def task_insert_results(**context):
    """Insert the column check results into the target BigQuery table."""
    ti = context["ti"]
    results = ti.xcom_pull(task_ids="perform_checks", key="results")
    if not results:
        logger.info("No results to insert.")
        return

    client = bigquery.Client(project=BQ_PROJECT_ID)
    quality_table = f"{BQ_PROJECT_ID}.{BQ_QUALITY_DATASET_NAME}.{BQ_TABLE_NAME}"
    rows_to_insert = [{
        "table_name": row["table_name"],
        "column_name": row["column_name"],
        "src": int(row["source"]),
        "tgt": int(row["target"]),
        "diff": int(row["diff"]),
        "ts": row["timestamp"],
        "status": str(row["status"])
    } for row in results]

    try:
        logger.info(f"Inserting {len(rows_to_insert)} rows into BigQuery table {quality_table}")
        errors = client.insert_rows_json(quality_table, rows_to_insert)
        if errors:
            logger.error(f"Insertion errors: {errors}")
            raise Exception(f"Insertion errors: {errors}")
        else:
            logger.info("Data quality results inserted successfully.")
    except Exception as e:
        logger.error(f"Error inserting results into BigQuery: {e}")

# Set default arguments before the DAG definition.
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 2, 13),
}

with DAG(
    "ecommerce_column_quality_check",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    create_quality_table_task = PythonOperator(
        task_id="create_quality_table",
        python_callable=create_quality_table,
    )

    list_files = PythonOperator(
        task_id="list_files",
        python_callable=task_list_files,
        provide_context=True,
    )

    list_bq_tables_task = PythonOperator(
        task_id="list_bq_tables",
        python_callable=task_list_bq_tables,
        provide_context=True,
    )

    perform_checks = PythonOperator(
        task_id="perform_checks",
        python_callable=task_perform_checks,
        provide_context=True,
    )

    insert_results = PythonOperator(
        task_id="insert_results",
        python_callable=task_insert_results,
        provide_context=True,
    )

    # Define task dependencies
    create_quality_table_task >> [list_files, list_bq_tables_task]
    [list_files, list_bq_tables_task] >> perform_checks >> insert_results
