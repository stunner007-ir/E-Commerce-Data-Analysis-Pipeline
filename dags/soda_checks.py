from airflow import DAG
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define default arguments
default_args = {
    "start_date": datetime(2023, 1, 1),
    "catchup": False,
}


@task.external_python(python="/usr/local/airflow/soda_venv/bin/python")
def run_soda_checks(scan_name, checks_subpath):
    from include.soda.checks.check_function import check

    return check(scan_name, checks_subpath)


@dag(
    dag_id="soda_checks",
    default_args=default_args,
    schedule_interval=None,  # No schedule, manual trigger
    tags=["example"],
)
def soda_checks_pipeline():
    """
    A DAG to run Soda checks.
    """

    check_load = PythonOperator(
        task_id="check_load",
        python_callable=run_soda_checks,
        op_kwargs={"scan_name": "check_load", "checks_subpath": "include/soda/checks"},
    )

    check_load


# Instantiate the DAG
soda_checks_dag = soda_checks_pipeline()
