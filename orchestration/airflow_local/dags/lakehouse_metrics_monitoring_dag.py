from __future__ import annotations

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


PROJECT_ROOT = "/opt/airflow/project"
METRICS_FILE = f"{PROJECT_ROOT}/data/lakehouse/governance/etl_metrics.json"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def validate_metrics_status() -> None:
    """Fail task khi ETL metrics không báo SUCCESS."""
    with open(METRICS_FILE, encoding="utf-8") as metrics_file:
        metrics = json.load(metrics_file)

    status = metrics.get("status")
    if status != "SUCCESS":
        raise ValueError(
            f"ETL metrics validation failed: expected status SUCCESS, got {status!r}."
        )

    print("ETL metrics status is SUCCESS. OK.")


with DAG(
    dag_id="lakehouse_metrics_monitoring_dag",
    description="Read local ETL metrics and fail when the job status is not SUCCESS.",
    default_args=default_args,
    start_date=datetime(2026, 7, 20),
    schedule=None,
    catchup=False,
    tags=["lakehouse", "local", "monitoring"],
) as dag:
    start = EmptyOperator(task_id="start")

    # Kiểm tra metrics đã được ETL job ghi ra chưa.
    check_metrics_file = BashOperator(
        task_id="check_metrics_file",
        bash_command=f"set -e; test -f {METRICS_FILE}; echo 'Metrics file exists.'",
    )

    # In metrics ra Airflow log để tiện theo dõi.
    print_metrics = BashOperator(
        task_id="print_metrics",
        bash_command=f"set -e; cat {METRICS_FILE}",
    )

    validate_metrics_status_task = PythonOperator(
        task_id="validate_metrics_status",
        python_callable=validate_metrics_status,
    )

    end = EmptyOperator(task_id="end")

    start >> check_metrics_file >> print_metrics >> validate_metrics_status_task >> end
