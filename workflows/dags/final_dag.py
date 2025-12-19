from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

BASE_DIR = "/opt/airflow/scripts/transformations/modelling"

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="run_modelling_scripts",
    default_args=default_args,
    description="Run modelling SQL and Python scripts",
    schedule_interval=None,  # manual trigger
    start_date=days_ago(1),
    catchup=False,
    tags=["modelling", "transformations"],
) as dag:

    run_dim_date_py = BashOperator(
        task_id="run_dim_date_ss_modelling_py",
        bash_command=f"python {BASE_DIR}/dim_date_ss_modelling.py",
    )

    run_dim_ss_sql = BashOperator(
        task_id="run_dim_ss_modelling_sql",
        bash_command=f"psql $DB_CONN -f {BASE_DIR}/dim_ss_modelling.sql",
    )

    run_dim_t2_sql = BashOperator(
        task_id="run_dim_t2_modelling_sql",
        bash_command=f"psql $DB_CONN -f {BASE_DIR}/dim_t2_modelling.sql",
    )

    run_fact_ss_sql = BashOperator(
        task_id="run_fact_ss_modelling_sql",
        bash_command=f"psql $DB_CONN -f {BASE_DIR}/fact_ss_modelling.sql",
    )

    run_fact_t2_sql = BashOperator(
        task_id="run_fact_t2_modelling_sql",
        bash_command=f"psql $DB_CONN -f {BASE_DIR}/fact_t2_modelling.sql",
    )

    # strict ordering: dims → facts
    run_dim_date_py >> run_dim_t2_sql >> run_dim_ss_sql >> run_fact_ss_sql >> run_fact_t2_sql