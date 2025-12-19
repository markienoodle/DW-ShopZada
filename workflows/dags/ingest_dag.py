from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'shopzada',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1)
}

with DAG(
    dag_id='shopzada_ingestion_pipeline',
    default_args=default_args,
    description='Ingest and Verify Shopzada Data',
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task 1: Install Dependencies
    install_deps = BashOperator(
        task_id='install_dependencies',
        bash_command='pip install pandas sqlalchemy psycopg2-binary openpyxl pyarrow lxml'
    )

    # Task 2: Run Ingestion
    run_ingest = BashOperator(
        task_id='run_ingest_script',
        bash_command='python /opt/airflow/scripts/ingest_script.py',
        retries=3,  # Try to load data 3 times
        retry_delay=timedelta(minutes=5) # Wait 5 minutes before retrying
    )

    # # Task 3: Verify Data (The New Step)
    # verify_data = BashOperator(
    #     task_id='verify_data_integrity',
    #     bash_command='python /opt/airflow/scripts/verify_script.py',
    #     retries=0
    # )

    # Set the order: Install -> Ingest -> Verifyxa
    install_deps >> run_ingest 
    # >> verify_data