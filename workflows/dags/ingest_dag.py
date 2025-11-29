from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default settings for the DAG
default_args = {
    'owner': 'shopzada',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

# Define the DAG context
with DAG(
    dag_id='shopzada_ingestion_pipeline',
    default_args=default_args,
    description='A simple DAG to ingest Shopzada data',
    schedule_interval='@daily', # Run once a day
    catchup=False # Don't run for past dates
) as dag:

    # Task 1: Install dependencies (Pandas, SQLAlchemy, etc.)
    # In a production environment, you would build these into the Docker image,
    # but for this project, installing them on the fly is easier.
    install_deps = BashOperator(
        task_id='install_dependencies',
        bash_command='pip install pandas sqlalchemy psycopg2-binary openpyxl pyarrow lxml'
    )

    # Task 2: Run your Ingest Script
    # We use the path /opt/airflow/scripts because that is where 
    # you mounted the folder in your docker-compose.yml
    run_ingest = BashOperator(
        task_id='run_ingest_script',
        bash_command='python /opt/airflow/scripts/ingest_script.py'
    )

    # Set the order: Install deps first, THEN run the script
    install_deps >> run_ingest