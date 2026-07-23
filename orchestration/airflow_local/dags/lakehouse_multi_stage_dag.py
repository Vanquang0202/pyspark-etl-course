from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


PROJECT_ROOT = "/opt/airflow/project"
GOVERNANCE_DIR = f"{PROJECT_ROOT}/data/lakehouse/governance"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="lakehouse_multi_stage_dag",
    description="Simulate a local production-style multi-stage lakehouse pipeline.",
    default_args=default_args,
    start_date=datetime(2026, 7, 20),
    schedule=None,
    catchup=False,
    tags=["lakehouse", "local", "pyspark", "multi-stage"],
) as dag:
    start = EmptyOperator(task_id="start")

    # Hiển thị thông tin batch do Airflow cung cấp.
    prepare_batch = BashOperator(
        task_id="prepare_batch",
        bash_command="""
        echo "Preparing local batch"
        echo "Execution date: {{ ds }}"
        echo "Run id: {{ run_id }}"
        """,
    )

    # Stage xử lý chính: chạy lại cùng PySpark lakehouse job local.
    run_full_lakehouse_job = BashOperator(
        task_id="run_full_lakehouse_job",
        bash_command=f"""
        set -e
        cd {PROJECT_ROOT}
        python projects/lakehouse_full_flow/lakehouse_job.py
        """,
    )

    # Xác nhận output dành cho BI/report đã tồn tại.
    validate_serving_output = BashOperator(
        task_id="validate_serving_output",
        bash_command=(
            f"set -e; test -d {PROJECT_ROOT}/data/lakehouse/serving/customer_summary_csv; "
            "echo 'Serving CSV output exists.'"
        ),
    )

    # Đây chỉ là mô phỏng publish, không gọi hệ thống bên ngoài.
    publish_report_mock = BashOperator(
        task_id="publish_report_mock",
        bash_command='echo "Mock publish serving report to BI/API/Report layer"',
    )

    # In metadata governance để kiểm tra stage cuối.
    governance_check = BashOperator(
        task_id="governance_check",
        bash_command=f"""
        set -e
        cat {GOVERNANCE_DIR}/catalog.json
        cat {GOVERNANCE_DIR}/lineage.json
        """,
    )

    end = EmptyOperator(task_id="end")

    (
        start
        >> prepare_batch
        >> run_full_lakehouse_job
        >> validate_serving_output
        >> publish_report_mock
        >> governance_check
        >> end
    )
