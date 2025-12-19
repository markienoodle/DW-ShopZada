from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule # <--- Added Import
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'transform_dag',
    default_args=default_args,
    description='Runs transform scripts; continues even if some fail',
    schedule_interval='@daily', 
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    start_task = EmptyOperator(task_id='start')
    end_task = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ALL_DONE # Ensure 'end' marks success even if tasks failed
    )

    # Update this path if needed
    SCRIPT_PATH = "/opt/airflow/scripts/transformations"

    tables_to_process = [
        'campaign_data',
        'order_data',
        'product_list',
        'user_data',
        'merchant_data',
        'staff_data',
        'line_item_data',
        'user_job',
        'user_credit_card',

        
    ]

    transform_task_groups = []

    for table_name in tables_to_process:
        
        # 1. Transform Task
        # trigger_rule='all_done' allows this to start even if the previous batch failed.
        transform_task = BashOperator(
            task_id=f'transform_{table_name}',
            bash_command=f'python {SCRIPT_PATH}/{table_name}.py',
            trigger_rule=TriggerRule.ALL_DONE 
        )

        transform_task >> end_task

        transform_task_groups.append(transform_task)

    # Batching Logic
    def chunk_list(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    batches = list(chunk_list(transform_task_groups, 2))

    # Connect Start -> First Batch
    start_task >> batches[0]

    # Connect Batch i -> Batch i+1
    for i in range(len(batches) - 1):
        current_batch = batches[i]
        next_batch = batches[i+1]
        
        for task in current_batch:
            task >> next_batch