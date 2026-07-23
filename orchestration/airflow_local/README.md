# Airflow Local - Lakehouse Full Flow

## Airflow là gì?

Airflow là tool orchestration.
Airflow quản lý luồng chạy job, schedule, retry, dependency và log.
Airflow không thay Spark để xử lý data.

## Trong project này Airflow làm gì?

- Trigger job `projects/lakehouse_full_flow/lakehouse_job.py`.
- Theo dõi job success/failed.
- Retry task nếu lỗi.
- Check output sau khi job chạy.
- In metrics từ `data/lakehouse/governance/etl_metrics.json` ra log.

## Mapping với kiến trúc thực tế

| Kiến trúc thực tế | Trong repo local |
|---|---|
| Airflow-triggered jobs | DAG `lakehouse_full_flow_dag` |
| Spark processing | `projects/lakehouse_full_flow/lakehouse_job.py` |
| Bronze/Silver/Gold | `data/lakehouse/` |
| Monitoring | Airflow UI + `etl_metrics.json` |
| Lineage/Quality | Governance JSON files |

## DAG gồm những task nào?

```text
start
-> check_project_path
-> run_lakehouse_job
-> check_outputs
-> show_metrics
-> end
```

Không đặt task tự xóa output trong DAG mặc định để tránh mất dữ liệu khi học.
Nếu muốn chạy lại sạch, dùng DAG `lakehouse_cleanup_dag` sau khi đã kiểm tra đúng path.

## Các DAG demo

- `lakehouse_full_flow_dag`: chạy full ETL job.
- `lakehouse_quality_check_dag`: kiểm tra output, quality report và lineage.
- `lakehouse_metrics_monitoring_dag`: đọc metrics và fail nếu `status` không phải `SUCCESS`.
- `lakehouse_multi_stage_dag`: mô phỏng pipeline nhiều stage.
- `lakehouse_cleanup_dag`: xóa output local để test lại.

Thứ tự test đề xuất trên Airflow UI:

1. Chạy `lakehouse_cleanup_dag` để xóa output cũ.
2. Chạy `lakehouse_full_flow_dag` hoặc `lakehouse_multi_stage_dag` để chạy ETL.
3. Chạy `lakehouse_quality_check_dag` để kiểm tra quality/lineage.
4. Chạy `lakehouse_metrics_monitoring_dag` để kiểm tra metrics.

Lưu ý: `etl_metrics.json` hiện có `"status": "FAILED"`, nên task `validate_metrics_status` của DAG monitoring sẽ fail theo đúng mục đích minh họa. Chạy ETL thành công để metrics được ghi lại `SUCCESS` trước khi mong đợi DAG này thành công.

## Cách Airflow trigger job

Task `run_lakehouse_job` dùng `BashOperator`:

```bash
cd /opt/airflow/project
python projects/lakehouse_full_flow/lakehouse_job.py
```

Trong Docker, repo hiện tại được mount vào:

```text
/opt/airflow/project
```

Nếu path mount sai trên máy bạn, sửa phần `volumes` trong `docker-compose.yml`.

## Lệnh chạy Airflow local trên Ubuntu/WSL

```bash
cd /mnt/e/PySpark/pyspark-etl-course/orchestration/airflow_local
cp .env.example .env
docker compose up airflow-init
docker compose up -d
```

Compose sẽ build một image local dựa trên `apache/airflow:2.10.5`, có thêm Java runtime và `pyspark` để chạy được PySpark job.

Mở UI:

```text
http://localhost:8080
```

Account/password mặc định:

```text
airflow / airflow
```

## Cách trigger DAG

1. Vào Airflow UI.
2. Bật DAG `lakehouse_full_flow_dag`.
3. Bấm Trigger DAG thủ công.
4. Xem log từng task.
5. Mở task `show_metrics` để xem nội dung `etl_metrics.json`.

## Cách tắt Airflow

```bash
docker compose down
```

Nếu muốn xóa cả database volume local của Airflow:

```bash
docker compose down -v
```

## Lưu ý

- Cần Docker Desktop bật WSL integration.
- Nếu bị permission denied Docker daemon, dùng `sudo docker compose` hoặc add user vào group `docker`.
- Nên chạy trên Ubuntu/WSL.
- Nếu mount repo sai, sửa `../../:/opt/airflow/project` trong `docker-compose.yml`.
- Đây là demo local để học cơ chế Airflow, không phải setup production-grade.
