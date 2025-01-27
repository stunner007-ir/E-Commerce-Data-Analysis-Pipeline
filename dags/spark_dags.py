from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'spark_data_quality_checks',
    default_args=default_args,
    description='Run Spark data quality checks',
    schedule_interval=None,  # Set to your desired schedule (e.g., '@daily')
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # BashOperator to run the Spark job
    run_spark_job = BashOperator(
        task_id='run_spark_job',
        bash_command='python3 /usr/local/airflow/Spark/data_quality/Trans_dim.py',  # Replace with the actual path to your script
    )

    run_spark_job
