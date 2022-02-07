
import os

from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from fhv_ingest import format_to_parquet, upload_to_gcs

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

dataset_url = f'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
OUTPUT_FILE_TEMPLATE = 'zones.csv'
PARQUET_FILE = 'zones.parquet'
PATH_PARQUET_FILE = AIRFLOW_HOME

# TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y-%m\') }}'

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="zones",
    default_args=default_args,
    catchup=True,
    max_active_runs=1
)as dag:

    download_dataset_task = BashOperator(
        task_id="download_data",
        retries=1,
        bash_command=f"curl -sSLf {dataset_url} > {AIRFLOW_HOME}/{OUTPUT_FILE_TEMPLATE}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="csv_to_parquet",
        retries=1,
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{AIRFLOW_HOME}/{OUTPUT_FILE_TEMPLATE}"
        }
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"{PARQUET_FILE}",
            "local_file": f"{PATH_PARQUET_FILE}/{PARQUET_FILE}",
        },
    )

    cleanup_task = BashOperator(
        task_id="cleanup_task",
        bash_command=f"rm {AIRFLOW_HOME}/{OUTPUT_FILE_TEMPLATE} {PATH_PARQUET_FILE}/{PARQUET_FILE}"
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> cleanup_task
