from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


LAKEHOUSE_DIR = "/opt/airflow/project/data/lakehouse"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="lakehouse_cleanup_dag",
    description="Delete local lakehouse outputs so the demo ETL can be tested again.",
    default_args=default_args,
    start_date=datetime(2026, 7, 20),
    schedule=None,
    catchup=False,
    tags=["lakehouse", "local", "cleanup"],
) as dag:
    start = EmptyOperator(task_id="start")

    # Hiển thị output hiện tại trước khi xóa.
    show_before_cleanup = BashOperator(
        task_id="show_before_cleanup",
        bash_command=f"ls -R {LAKEHOUSE_DIR} || true",
    )

    # Chỉ dùng cho demo local; production không được xóa output tùy tiện.
    cleanup_lakehouse_output = BashOperator(
        task_id="cleanup_lakehouse_output",
        bash_command=f"""
        # DEMO LOCAL ONLY: production cần retention policy và phê duyệt trước khi xóa.
        rm -rf {LAKEHOUSE_DIR}
        echo "Removed local lakehouse output: {LAKEHOUSE_DIR}"
        """,
    )

    # Xác nhận trạng thái thư mục sau cleanup.
    show_after_cleanup = BashOperator(
        task_id="show_after_cleanup",
        bash_command=f"ls -R {LAKEHOUSE_DIR} || true",
    )

    end = EmptyOperator(task_id="end")

    start >> show_before_cleanup >> cleanup_lakehouse_output >> show_after_cleanup >> end
