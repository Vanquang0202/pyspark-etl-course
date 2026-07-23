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
    dag_id="lakehouse_quality_check_dag",
    description="Check local lakehouse outputs and governance reports.",
    default_args=default_args,
    start_date=datetime(2026, 7, 20),
    schedule=None,
    catchup=False,
    tags=["lakehouse", "local", "pyspark", "quality"],
) as dag:
    start = EmptyOperator(task_id="start")

    # Kiểm tra các output mà full ETL job cần tạo ra.
    check_lakehouse_output_exists = BashOperator(
        task_id="check_lakehouse_output_exists",
        bash_command=f"""
        set -e
        test -d {PROJECT_ROOT}/data/lakehouse/bronze/customer_events_raw
        test -d {PROJECT_ROOT}/data/lakehouse/silver/customer_clean
        test -d {PROJECT_ROOT}/data/lakehouse/silver/customer_invalid
        test -d {PROJECT_ROOT}/data/lakehouse/gold/customer_summary_by_province
        test -d {PROJECT_ROOT}/data/lakehouse/serving/customer_summary_csv
        test -d {GOVERNANCE_DIR}
        echo "Lakehouse outputs are present."
        """,
    )

    # In báo cáo chất lượng dữ liệu vào task log.
    show_data_quality_report = BashOperator(
        task_id="show_data_quality_report",
        bash_command=f"set -e; cat {GOVERNANCE_DIR}/data_quality_report.json",
    )

    # In lineage để xem luồng dữ liệu giữa các layer.
    show_lineage = BashOperator(
        task_id="show_lineage",
        bash_command=f"set -e; cat {GOVERNANCE_DIR}/lineage.json",
    )

    end = EmptyOperator(task_id="end")

    start >> check_lakehouse_output_exists >> show_data_quality_report >> show_lineage >> end
