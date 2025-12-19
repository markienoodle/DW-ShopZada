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

with DAG(
    'deduplication_pipeline',
    default_args=default_args,
    description='Runs deduplication scripts simultaneously',
    schedule_interval='@daily', # Adjust schedule as needed
    start_date=datetime(2023, 1, 1),
    catchup=False,
    # IMPORTANT: Point this to where your 'scripts' folder lives in your Airflow environment
    template_searchpath=['/opt/airflow/scripts/deduplications'] 
) as dag:

    # 1. Create Start and End markers (Good practice for visualization)
    start_task = EmptyOperator(task_id='start')
    end_task = EmptyOperator(task_id='end')

    # 2. Define the SQL tasks
    # We use the filenames you provided. Airflow will look for them in the template_searchpath defined above.
    
    dedup_campaign = PostgresOperator(
        task_id='dedup_campaign',
        postgres_conn_id='your_db_connection_id', # Change this to your connection ID
        sql='campaign_data_dedup.sql'
    )

    dedup_line_item = PostgresOperator(
        task_id='dedup_line_item',
        postgres_conn_id='your_db_connection_id',
        sql='line_item_data_dedup.sql'
    )

    dedup_order = PostgresOperator(
        task_id='dedup_order',
        postgres_conn_id='your_db_connection_id',
        sql='order_data_dedup.sql'
    )

    dedup_product = PostgresOperator(
        task_id='dedup_product',
        postgres_conn_id='your_db_connection_id',
        sql='product_list_dedup.sql'
    )

    # 3. Set Dependencies
    # By putting the tasks in a list [], Airflow knows to run them in parallel
    start_task >> [dedup_campaign, dedup_line_item, dedup_order, dedup_product] >> end_task