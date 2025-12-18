from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# ==========================================
# CONFIGURATION
# ==========================================
TRANSFORM_SCRIPTS_DIR = "/opt/airflow/scripts/transformations"

default_args = {
    'owner': 'shopzada',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
    'catchup': False
}

# ==========================================
# 1. DEFINE TARGET TABLES
# ==========================================
# This list must match the PREFIX of your filenames in the screenshot.
# e.g., for "order_data.py", the item here is "order_data"
target_tables = [
    'campaign_data',
    'order_data',
    'order_with_merchant_data',
    'product_list',
    'transactional_campaign_data',
    'user_data',
    'merchant_data',
    'staff_data',
    'line_item_data_prices',
    'line_item_data_products'
]

# ==========================================
# 2. DAG DEFINITION
# ==========================================
with DAG(
    dag_id='shopzada_transformation_layer_1',
    default_args=default_args,
    description='Run transform and verify scripts matching the folder structure',
    schedule_interval='@daily',
    tags=['transform', 'layer1', 'grouped']
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    for table in target_tables:
        
        # 1. Transform Task
        # Matches "order_data.py"
        transform_task = BashOperator(
            task_id=f'transform_{table}',
            bash_command=f'python {TRANSFORM_SCRIPTS_DIR}/{table}.py'
        )

        # 2. Verify Task
        # Matches "order_data_verify.py"
        verify_task = BashOperator(
            task_id=f'verify_{table}',
            bash_command=f'python {TRANSFORM_SCRIPTS_DIR}/{table}_verify.py'
        )

        # 3. Dependency: Start -> Transform -> Verify -> End
        start >> transform_task >> verify_task >> end