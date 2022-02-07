
import os

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from fhv_ingest import format_to_parquet, upload_to_gcs

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

dataset_file = "fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv"
dataset_url = f'https://s3.amazonaws.com/nyc-tlc/trip+data/{dataset_file}'
OUTPUT_FILE_TEMPLATE = 'fhv_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
PARQUET_FILE = 'fhv_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
PATH_PARQUET_FILE = AIRFLOW_HOME

# TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y-%m\') }}'

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="fhv_data_v1",
    default_args=default_args,
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020,1,1),
    schedule_interval="0 6 2 * *",
    catchup=True,
    max_active_runs=3
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
            "object_name": f"fhv/{PARQUET_FILE}",
            "local_file": f"{PATH_PARQUET_FILE}/{PARQUET_FILE}",
        },
    )

    cleanup_task = BashOperator(
        task_id="cleanup_task",
        bash_command=f"rm {AIRFLOW_HOME}/{OUTPUT_FILE_TEMPLATE} {PATH_PARQUET_FILE}/{PARQUET_FILE}"
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> cleanup_task
