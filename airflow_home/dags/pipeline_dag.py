from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

#PROJECT_ROOT = r"E:\git\glucose-data-platform"
PROJECT_ROOT = "/project"


with DAG(
    dag_id="glucose_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # run manually
    catchup=False,
    tags=["glucose", "lakehouse"],
) as dag:

    dq_check = BashOperator(
        task_id="data_quality",
        bash_command=f"cd {PROJECT_ROOT} && python warehouse/data_quality.py",
    )

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=f"cd {PROJECT_ROOT} && python warehouse/build_gold.py",
    )

    dq_check >> build_gold
