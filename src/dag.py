import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import subprocess

# Default parameters for the DAG
default_args = {
    "owner": "me",
    "start_date": airflow.utils.dates.days_ago(2),
    "schedule_interval": "@daily",
}

# Create the DAG
dag = DAG(
    "door2door_data_pipeline",
    default_args=default_args,
    description="Data processing pipeline for door2door vehicle tracking data",
)

# Define the fetch_data task
def fetch_data(**kwargs):
    host = kwargs["host"]
    user = kwargs["user"]
    password = kwargs["password"]
    bucket = kwargs["bucket"]
    date = kwargs["date"]
    # Call the fetch_data.py script
    subprocess.call(["python", "src/fetch_data.py", host, user, password, bucket, date])

    # Create the fetch_data Operator
    fetch_data_task = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_data,
        op_kwargs={
            "host": host,
            "user": user,
            "password": password,
            "bucket": bucket,
            "date": date,
        },
    )


# Define the process_data task
def process_data(**kwargs):
    date = kwargs["date"]
    # Call the process_data.py script
    subprocess.call(["python", "src/process_data.py", date])

    # Create the process_data Operator
    process_data_task = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
        op_kwargs={"date": date},
        dag=dag,
    )


# Define the transform_data task
def transform_data(**kwargs):
    date = kwargs["date"]
    # Call the transform_data.py script
    subprocess.call(["python", "src/transform_data.py", date])

    # Create the transform_data Operator
    transform_data_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        op_kwargs={"date": date},
        dag=dag,
    )


# Define the store_data task
def store_data(**kwargs):
    host = kwargs["host"]
    user = kwargs["user"]
    password = kwargs["password"]
    database = kwargs["database"]
    date = kwargs["date"]
    database_type = kwargs["database_type"]
    # Call the store_data.py script
    subprocess.call(
        [
            "python",
            "src/store_data.py",
            host,
            user,
            password,
            database,
            date,
            database_type,
        ]
    )

    # Create the store_data Operator
    store_data_task = PythonOperator(
        task_id="store_data",
        python_callable=store_data,
        op_kwargs={
            "host": host,
            "user": user,
            "password": password,
            "database": database,
            "date": date,
            "database_type": database_type,
        },
        dag=dag,
    )


# Set up dependencies between the tasks
fetch_data_task >> process_data_task >> transform_data_task >> store_data_task
