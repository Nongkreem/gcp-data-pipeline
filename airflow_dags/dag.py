from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

PROJECT_ID = 'my-spark-project-463102'
CLUSTER_NAME = 'spark-jupyter-2'
REGION = 'asia-southeast1'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 12),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('bank_data',
          default_args=default_args,
          description='Runs an external Python script',
          schedule_interval='@daily',
          catchup=False)

with dag:
    run_script_task = BashOperator(
        task_id='extract_data',
        bash_command='python /home/airflow/gcs/dags/scripts/raw_data_to_gcs.py',
    )

    pyspark_job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": "gs://gcs-data-marketing-463102/notebooks/jupyter/scripts/etl_to_bigquery.py",
        },
    }

    submit_pyspark = DataprocSubmitJobOperator(
        task_id='submit_pyspark_job',
        job=pyspark_job,
        region=REGION,
        project_id=PROJECT_ID,
    )

    run_script_task >> submit_pyspark