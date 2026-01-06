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

# Define the Postgres connection ID
DB_CONNECTION_ID = 'airflow_db'

with DAG(
    'deduplication_pipeline',
    default_args=default_args,
    description='Runs deduplication scripts simultaneously on Postgres',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    template_searchpath=['/opt/airflow/scripts/deduplications'] 
) as dag:

    # Start and End markers
    start_task = EmptyOperator(task_id='start')
    end_task = EmptyOperator(task_id='end')

    # Deduplication tasks
    dedup_campaign = PostgresOperator(
        task_id='dedup_campaign',
        postgres_conn_id=DB_CONNECTION_ID,
        sql='campaign_data_dedup.sql'
    )

    dedup_line_item = PostgresOperator(
        task_id='dedup_line_item',
        postgres_conn_id=DB_CONNECTION_ID,
        sql='line_item_data_dedup.sql'
    )

    dedup_merchant = PostgresOperator(
        task_id='dedup_merchant',
        postgres_conn_id=DB_CONNECTION_ID,
        sql='merchant_data_dedup.sql'
    )

    dedup_order = PostgresOperator(
        task_id='dedup_order',
        postgres_conn_id=DB_CONNECTION_ID,
        sql='order_data_dedup.sql'
    )

    dedup_product = PostgresOperator(
        task_id='dedup_product',
        postgres_conn_id=DB_CONNECTION_ID,
        sql='product_list_dedup.sql'
    )

    dedup_staff = PostgresOperator(
        task_id='dedup_staff',
        postgres_conn_id=DB_CONNECTION_ID,
        sql='staff_data_dedup.sql'
    )

    dedup_user_credit_card = PostgresOperator(
        task_id='dedup_user_credit_card',
        postgres_conn_id=DB_CONNECTION_ID,
        sql='user_credit_card_dedup.sql'
    )

    dedup_user = PostgresOperator(
        task_id='dedup_user',
        postgres_conn_id=DB_CONNECTION_ID,
        sql='user_data_dedup.sql'
    )

    dedup_user_job = PostgresOperator(
        task_id='dedup_user_job',
        postgres_conn_id=DB_CONNECTION_ID,
        sql='user_job_dedup.sql'
    )

    # Set dependencies: all dedup tasks run in parallel
    start_task >> [
        dedup_campaign,
        dedup_line_item,
        dedup_merchant,
        dedup_order,
        dedup_product,
        dedup_staff,
        dedup_user_credit_card,
        dedup_user,
        dedup_user_job
    ] >> end_task
