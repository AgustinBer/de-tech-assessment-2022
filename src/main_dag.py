from airflow import DAG
from airflow.operators.python import PythonOperator
from fetch_data import fetch_data
from process_data import process_data
from store_data import store_data
from transform_data import transform_data
import os

import datetime as dt

from dotenv import load_dotenv

load_dotenv()


env_vars = {
    "host": os.environ["REDSHIFT_HOST"],
    "port": os.environ["REDSHIFT_PORT"],
    "user": os.environ["REDSHIFT_USER"],
    "password": os.environ["REDSHIFT_PASSWORD"],
    "database": os.environ["REDSHIFT_DATABASE"],
    "bucket": os.environ["S3_BUCKET"],
}


# Default parameters for the DAG
default_args = {
    "owner": "me",
    "start_date": dt.datetime(2023, 1, 1),
    "schedule_interval": "@daily",
}

# Create the DAG
dag = DAG(
    "door2door_data_pipeline",
    default_args=default_args,
    description="Data processing pipeline for door2door vehicle tracking data",
)

# Create the fetch_data Operator
fetch_data_task = PythonOperator(
    task_id="fetch_data",
    python_callable=fetch_data,
    op_kwargs={
        "host": env_vars["host"],
        "user": env_vars["user"],
        "password": env_vars["password"],
        "bucket": env_vars["bucket"],
    },
    dag=dag,
)

# Create the process_data Operator
process_data_task = PythonOperator(
    task_id="process_data",
    python_callable=process_data,
    dag=dag,
)


# Create the transform_data Operator
transform_data_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag,
)


# Create the store_data Operator
store_data_task = PythonOperator(
    task_id="store_data",
    python_callable=store_data,
    op_kwargs={
        "host": env_vars["host"],
        "user": env_vars["user"],
        "password": env_vars["password"],
        "database": env_vars["database"],
    },
    dag=dag,
)


# Set up dependencies between the tasks
fetch_data_task >> process_data_task >> transform_data_task >> store_data_task
