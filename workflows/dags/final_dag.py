from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import subprocess
import sys
import os

BASE_DIR = "/opt/airflow/scripts/modelling"

default_args = {
    "owner": "airflow",
    "retries": 1,
}

def run_python_script(script_path, **kwargs):
    """Execute a Python script safely and transparently."""

    if not os.path.exists(script_path):
        parent_dir = os.path.dirname(script_path)
        existing_files = (
            os.listdir(parent_dir) if os.path.exists(parent_dir) else "Directory not found"
        )
        raise FileNotFoundError(
            f"Script not found: {script_path}\n"
            f"Contents of {parent_dir}: {existing_files}"
        )

    result = subprocess.run(
        [sys.executable, script_path],
        cwd=BASE_DIR,              # ✅ ensure relative imports & configs work
        env=os.environ.copy(),     # ✅ inherit Airflow env (DB creds, etc.)
        capture_output=True,
        text=True,
    )

    # Always print stdout so it appears in Airflow logs
    if result.stdout:
        print("===== STDOUT =====")
        print(result.stdout)

    if result.stderr:
        print("===== STDERR =====")
        print(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(
            f"Script failed with exit code {result.returncode}\n"
            f"Script: {script_path}"
        )

with DAG(
    dag_id="run_modelling_scripts",
    default_args=default_args,
    description="Run modelling Python scripts",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["modelling", "transformations"],
) as dag:

    run_dim_date_py = PythonOperator(
        task_id="run_dim_date_ss_modelling_py",
        python_callable=run_python_script,
        op_kwargs={"script_path": os.path.join(BASE_DIR, "dim_date_ss_modelling.py")},
    )

    run_dim_ss_sql = PythonOperator(
        task_id="run_dim_ss_modelling_sql",
        python_callable=run_python_script,
        op_kwargs={"script_path": os.path.join(BASE_DIR, "dim_ss_modelling.py")},
    )

    run_dim_t2_sql = PythonOperator(
        task_id="run_dim_t2_modelling_sql",
        python_callable=run_python_script,
        op_kwargs={"script_path": os.path.join(BASE_DIR, "dim_t2_modelling.py")},
    )

    run_fact_ss_sql = PythonOperator(
        task_id="run_fact_ss_modelling_sql",
        python_callable=run_python_script,
        op_kwargs={"script_path": os.path.join(BASE_DIR, "fact_ss_modelling.py")},
    )

    # strict ordering: dims → facts
    run_dim_date_py >> run_dim_t2_sql >> run_dim_ss_sql >> run_fact_ss_sql
