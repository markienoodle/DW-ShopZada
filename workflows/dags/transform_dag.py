from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import os

# ==========================================
# CONFIGURATION
# ==========================================
# The folder where your transform scripts live (inside the container)
TRANSFORM_SCRIPTS_DIR = "/opt/airflow/scripts/transformations"

default_args = {
    'owner': 'shopzada',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
    'catchup': False
}

# ==========================================
# HELPER: Generate Table Names from File List
# ==========================================
# We use the same list from your verify script to ensure 1:1 mapping
raw_files = [
    'product_list.xlsx',
    'order_with_merchant_data1.parquet',
    'order_with_merchant_data2.parquet',
    'order_with_merchant_data3.csv',
    'merchant_data.html',
    'staff_data.html',
    'line_item_data_prices1.csv',
    'line_item_data_prices2.csv',
    'line_item_data_prices3.parquet',
    'line_item_data_products1.csv',
    'line_item_data_products2.csv',
    'line_item_data_products3.parquet',
    'order_data_20200101-20200701.parquet',
    'order_data_20200701-20211001.pickle',
    'order_data_20211001-20220101.csv',
    'order_data_20220101-20221201.xlsx',
    'order_data_20221201-20230601.json',
    'order_data_20230601-20240101.html',
    'order_delays.html',
    'user_job.csv',
    'user_data.json',
    'user_credit_card.pickle',
    'campaign_data.csv',
    'transactional_campaign_data.csv'
]

# Convert filenames to table names (e.g., "file-name.csv" -> "file_name_csv")
table_names = [f.replace('.', '_').replace('-', '_') for f in raw_files]

# ==========================================
# DAG DEFINITION
# ==========================================
with DAG(
    dag_id='shopzada_transformation_layer_1',
    default_args=default_args,
    description='Run individual transformation scripts for raw tables',
    schedule_interval='@daily', # You can change this to None to trigger manually
    tags=['transform', 'layer1']
) as dag:

    start = EmptyOperator(task_id='start_transformations')
    end = EmptyOperator(task_id='end_transformations')

    # Loop through every table and create a task dynamically
    for table in table_names:
        
        # Construct the expected script name
        # Example: transform1__product_list_xlsx.py
        script_name = f"transform1__{table}.py"
        
        # Check if file exists to prevent immediate failure? 
        # In Airflow, we usually assume the code exists. 
        # If the file is missing, the BashOperator will fail (which is good, it alerts you).
        
        transform_task = BashOperator(
            task_id=f'transform_{table}',
            bash_command=f'python {TRANSFORM_SCRIPTS_DIR}/{script_name}'
        )

        # Set dependencies: Start -> Transform Task -> End
        # This runs all scripts in PARALLEL (much faster!)
        start >> transform_task >> end