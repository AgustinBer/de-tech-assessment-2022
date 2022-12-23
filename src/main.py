import airflow
from my_dag_file import dag
from my_database_module import setup_database_connection

if __name__ == "__main__":
    # Set up the database connection
    setup_database_connection()

    # Start the DAG
    dag.cli()
