# Lakehouse Full Flow - PySpark Local

## Project này mô phỏng gì?

Project này mô phỏng một flow Data Lakehouse ETL đầy đủ bằng PySpark local.
Mục tiêu là hiểu luồng Source -> Ingestion -> Storage -> Table Format -> Catalog -> Processing -> Bronze/Silver/Gold -> Serving -> Governance.

Không cần chạy Oracle, API, Kafka, Airflow, MinIO, Iceberg, Nessie, Glue hay Unity Catalog thật.
Tất cả được mapping sang folder và file local để học.

## Mapping kiến trúc thật sang local

| Kiến trúc thật | Mô phỏng local |
|---|---|
| Oracle / API / Kafka / File | CSV/JSON trong `data/input/lakehouse_full_flow/` |
| Spark / Kafka / CDC / Airflow-triggered jobs | PySpark job local |
| Ceph / S3 / MinIO / ADLS | `data/lakehouse/` |
| Iceberg / Delta / Hudi | Parquet |
| Nessie / Hive Metastore / Glue / Unity Catalog | `data/lakehouse/governance/catalog.json` |
| Spark / Trino / Flink / dbt | Spark processing |
| Bronze / Silver / Gold | `data/lakehouse/bronze`, `silver`, `gold` |
| BI Dashboard / API / Report / ML | `data/lakehouse/serving/customer_summary_csv/` |
| Metadata / Lineage / Quality / Security | JSON trong `data/lakehouse/governance/` |

## Bronze / Silver / Gold

- Bronze: giữ dữ liệu gần raw nhất, thêm metadata như `ingestion_time`, `batch_id`, `source_file`.
- Silver: clean/cast dữ liệu, tách valid và invalid records.
- Gold: aggregate dữ liệu sạch để phục vụ report.

## Job chạy qua những bước nào?

1. Đọc source CSV và JSON local.
2. Ghi raw data vào Bronze.
3. Clean/cast dữ liệu từ Bronze sang Silver.
4. Validate `customer_id`, `province_code`, `amount`.
5. Tách valid vào `silver/customer_clean`.
6. Tách invalid vào `silver/customer_invalid`.
7. Aggregate valid records theo `province_code`.
8. Ghi Gold dạng Parquet.
9. Ghi Serving report dạng CSV.
10. Ghi governance JSON files.

## Output dự kiến

Khi chạy job, output sẽ nằm trong:

```text
data/lakehouse/
|-- bronze/customer_events_raw/
|-- silver/customer_clean/
|-- silver/customer_invalid/
|-- gold/customer_summary_by_province/
|-- serving/customer_summary_csv/
`-- governance/
    |-- catalog.json
    |-- lineage.json
    |-- data_quality_report.json
    `-- etl_metrics.json
```

## Governance files

- `catalog.json`: mô tả dataset name, layer, path và description.
- `lineage.json`: mô tả flow source -> bronze -> silver -> gold -> serving.
- `data_quality_report.json`: total, valid, invalid và summary theo `invalid_reason`.
- `etl_metrics.json`: job name, batch id, start/end time, duration và status.

## Lệnh chạy trên Ubuntu/WSL

```bash
cd /mnt/e/PySpark/pyspark-etl-course
source .venv-linux/bin/activate
python projects/lakehouse_full_flow/lakehouse_job.py
```

Có thể truyền tham số:

```bash
python projects/lakehouse_full_flow/lakehouse_job.py \
  --batch-id batch_20260720_001 \
  --run-date 2026-07-20 \
  --write-mode overwrite
```

Hoặc chạy script:

```bash
bash projects/lakehouse_full_flow/run_local.sh
```

## Lệnh xem output

```bash
ls -R data/lakehouse
cat data/lakehouse/governance/catalog.json
cat data/lakehouse/governance/lineage.json
cat data/lakehouse/governance/data_quality_report.json
cat data/lakehouse/governance/etl_metrics.json
cat data/lakehouse/serving/customer_summary_csv/part-*.csv
```

## Note

Nên chạy Spark write trên Ubuntu/WSL để tránh lỗi Hadoop/winutils trên Windows.
Project này chỉ mô phỏng kiến trúc lakehouse bằng local folder, không yêu cầu service cloud hay catalog thật.
