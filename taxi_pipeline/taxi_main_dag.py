from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# importing the functions from the tasks file
from scripts.ingest import ingest_taxi_data
from scripts.clean import clean_taxi_data
from scripts.transform import transform_taxi_data
from scripts.load import load_taxi_model

# defining default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 3, 1),
    'retries': 1,
}

with DAG('taxi_pipeline', default_args=default_args, description='A DAG for the taxi data pipeline', schedule=None) as dag:
    # defining the tasks
    ingest_task = PythonOperator(
        task_id='ingest_taxi_data',
        python_callable=ingest_taxi_data
    )

    clean_task = PythonOperator(
        task_id='clean_taxi_data',
        python_callable=clean_taxi_data
    )

    transform_task = PythonOperator(
        task_id='transform_taxi_data',
        python_callable=transform_taxi_data
    )

    load_task = PythonOperator(
        task_id='load_taxi_model',
        python_callable=load_taxi_model
    )

    # setting up the dependencies between the tasks
    ingest_task >> clean_task >> transform_task >> load_task