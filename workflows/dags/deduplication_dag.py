from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the connection ID here. 
# 'postgres_default' is the standard ID Airflow uses for Postgres.
# Make sure your 'staging1_schema' exists in the database this connection points to.
DB_CONNECTION_ID = 'airflow_db'

with DAG(
    'deduplication_pipeline',
    default_args=default_args,
    description='Runs deduplication scripts simultaneously on Postgres',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    # Ensure this path matches where your SQL files are located
    template_searchpath=['/opt/airflow/scripts/deduplications'] 
) as dag:

    # 1. Create Start and End markers
    start_task = EmptyOperator(task_id='start')
    end_task = EmptyOperator(task_id='end')

    # 2. Define the SQL tasks using the Postgres Connection
    dedup_campaign = PostgresOperator(
        task_id='dedup_campaign',
        postgres_conn_id=DB_CONNECTION_ID,  # UPDATED: Points to Postgres
        sql='campaign_data_dedup.sql'
    )

    dedup_line_item = PostgresOperator(
        task_id='dedup_line_item',
        postgres_conn_id=DB_CONNECTION_ID,  # UPDATED: Points to Postgres
        sql='line_item_data_dedup.sql'
    )

    dedup_order = PostgresOperator(
        task_id='dedup_order',
        postgres_conn_id=DB_CONNECTION_ID,  # UPDATED: Points to Postgres
        sql='order_data_dedup.sql'
    )

    dedup_product = PostgresOperator(
        task_id='dedup_product',
        postgres_conn_id=DB_CONNECTION_ID,  # UPDATED: Points to Postgres
        sql='product_list_dedup.sql'
    )

    # 3. Set Dependencies
    start_task >> [dedup_campaign, dedup_line_item, dedup_order, dedup_product] >> end_task