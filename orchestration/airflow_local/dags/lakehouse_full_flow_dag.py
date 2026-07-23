from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


PROJECT_ROOT = "/opt/airflow/project"
LAKEHOUSE_JOB = f"{PROJECT_ROOT}/projects/lakehouse_full_flow/lakehouse_job.py"


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="lakehouse_full_flow_dag",
    description="Trigger local PySpark lakehouse full flow job.",
    default_args=default_args,
    start_date=datetime(2026, 7, 20),
    schedule=None,
    catchup=False,
    tags=["pyspark", "lakehouse", "local"],
) as dag:
    start = EmptyOperator(task_id="start")

    # Kiểm tra repo đã được mount đúng vào container Airflow.
    check_project_path = BashOperator(
        task_id="check_project_path",
        bash_command=f"""
        set -e
        test -d {PROJECT_ROOT}
        test -f {LAKEHOUSE_JOB}
        echo "Project path is OK: {PROJECT_ROOT}"
        """,
    )

    # Airflow chỉ trigger Python/Spark job, không trực tiếp xử lý data.
    run_lakehouse_job = BashOperator(
        task_id="run_lakehouse_job",
        bash_command=f"""
        set -e
        cd {PROJECT_ROOT}
        python projects/lakehouse_full_flow/lakehouse_job.py
        """,
    )

    # Kiểm tra các layer/output chính sau khi job chạy.
    check_outputs = BashOperator(
        task_id="check_outputs",
        bash_command=f"""
        set -e
        test -d {PROJECT_ROOT}/data/lakehouse/bronze/customer_events_raw
        test -d {PROJECT_ROOT}/data/lakehouse/silver/customer_clean
        test -d {PROJECT_ROOT}/data/lakehouse/gold/customer_summary_by_province
        test -d {PROJECT_ROOT}/data/lakehouse/serving/customer_summary_csv
        test -d {PROJECT_ROOT}/data/lakehouse/governance
        echo "Lakehouse outputs are OK."
        """,
    )

    # In metrics ra Airflow task log để dễ monitor.
    show_metrics = BashOperator(
        task_id="show_metrics",
        bash_command=f"""
        set -e
        cat {PROJECT_ROOT}/data/lakehouse/governance/etl_metrics.json
        """,
    )

    end = EmptyOperator(task_id="end")

    start >> check_project_path >> run_lakehouse_job >> check_outputs >> show_metrics >> end
