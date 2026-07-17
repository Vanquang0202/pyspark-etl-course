# Final Project - Customer ETL Job

## Mục tiêu

Final project mô phỏng một PySpark ETL job gần với thực tế:

```text
Extract CSV input
-> Validate / Cast / Clean
-> Join mapping danh mục tỉnh
-> Tách valid / invalid records
-> Tạo report tổng hợp
-> Ghi output CSV / Parquet
-> In summary rõ ràng
```

Core ETL dùng PySpark DataFrame API để nối lại kiến thức từ các chapter trước.

## File input

Job ưu tiên đọc file raw riêng cho final project:

```text
data/input/final_project/customer_raw.csv
```

File này có cả dữ liệu tốt và dữ liệu lỗi như:

- `amount` không phải số.
- `amount` âm.
- `birth_date` sai format.
- `customer_id` null.
- `province_code` null.
- `province_code` không mapping được với `province.csv`.
- `name` có khoảng trắng, chữ thường và chữ hoa lẫn lộn.

Nếu file raw này không tồn tại, job fallback về:

```text
data/input/customer.csv
```

Bảng danh mục tỉnh:

```text
data/input/province.csv
```

## Folder output

Tất cả output được ghi vào:

```text
data/output/final_project/
```

Các folder output:

```text
valid_customers_parquet/
invalid_customers_csv/
province_segment_report_csv/
etl_summary_csv/
```

## Luồng xử lý ETL

1. Đọc customer raw CSV và province CSV.
2. In schema input.
3. Trim và chuẩn hóa `customer_name` bằng `initcap()`.
4. Chuẩn hóa `province_code`.
5. Giữ `customer_id` dạng string để bảo toàn mã có số 0 ở đầu như `001`.
6. Cast `amount` sang double.
7. Parse `birth_date` bằng format `dd/MM/yyyy`.
8. Tạo `processing_date`.
9. Left join customer với province theo `province_code`.
10. Tạo `mapping_status`.
11. Tạo `data_quality_status`.
12. Tách `valid_df` và `invalid_df`.
13. Tạo `customer_segment` trên valid records.
14. Tạo report theo `province_name` và `customer_segment`.
15. Ghi valid, invalid, report và summary ra output.

## Rule data quality

Job lấy lỗi đầu tiên theo thứ tự ưu tiên:

```text
MISSING_CUSTOMER_ID
MISSING_NAME
MISSING_PROVINCE_CODE
INVALID_AMOUNT
INVALID_BIRTH_DATE
UNMAPPED_PROVINCE
OK
```

`valid_df` chỉ gồm record có `data_quality_status = OK`.

`invalid_df` gồm các cột raw để dễ điều tra lỗi:

```text
customer_id_raw, name_raw, birth_date_raw, province_code_raw, amount_raw,
mapping_status, data_quality_status, processing_date
```

## Report

Report được tạo từ valid records:

```text
groupBy province_name, customer_segment
```

Các chỉ số:

```text
customer_count
total_amount
avg_amount
max_amount
min_amount
```

## Cách chạy trên Ubuntu/WSL

```bash
cd /mnt/e/PySpark/pyspark-etl-course
source .venv-linux/bin/activate
python final_project/etl_job.py
```

## Cách kiểm tra output

```bash
ls -R data/output/final_project
cat data/output/final_project/invalid_customers_csv/part-*.csv
cat data/output/final_project/province_segment_report_csv/part-*.csv
cat data/output/final_project/etl_summary_csv/part-*.csv
```

Có thể đọc Parquet bằng Spark:

```bash
python -c "from shared.spark_utils import create_spark_session; from shared.path_utils import DATA_OUTPUT; spark=create_spark_session('check-final-output'); spark.read.parquet(str(DATA_OUTPUT / 'final_project' / 'valid_customers_parquet')).show(truncate=False); spark.stop()"
```

## Vì sao Spark output là folder?

Spark xử lý dữ liệu phân tán theo partition. Khi ghi CSV hoặc Parquet, Spark tạo một folder output, bên trong có các file `part-*` tương ứng với partition output.

File `_SUCCESS` là marker cho biết job write đã hoàn tất thành công.

Vì vậy khi kiểm tra CSV output, cần đọc:

```text
data/output/final_project/<output_name>/part-*.csv
```

không phải đọc trực tiếp tên folder như một file CSV đơn.

## Hướng nâng cấp tiếp theo

Các hướng nâng cấp tiếp theo cho final project:

- JDBC read/write để đọc source từ `VSS_ODS` và ghi kết quả sang `VSS_360`.
- Window function để lấy bản ghi mới nhất theo `customer_id`.
- Broadcast join cho bảng danh mục nhỏ như `province`.
- Partition output theo `processing_date` hoặc `province`.
- Cache `quality_df` nếu DataFrame này được dùng bởi nhiều action.
- Structured Streaming nếu source là Kafka/event thay vì file batch.

## Có thể phát triển tiếp

- Tối ưu performance bằng `explain()` plan và giảm shuffle.
- Xử lý incremental ETL bằng watermark.
- Đọc/ghi database thật qua JDBC.
- Đọc event từ Kafka bằng Structured Streaming.
- Thêm checkpoint cho streaming job.
- Bổ sung unit test, config-driven ETL và rule data quality nâng cao như các chapter 24-30.

## Quick Notes

### Bài này học gì?

* Ghép các bước ETL thành một job end-to-end.
* Tách dữ liệu valid/invalid và tạo report tổng hợp.
* Ghi output phục vụ kiểm tra lỗi, báo cáo và summary.

### Khái niệm chính

* Extract: đọc customer raw và province mapping từ input.
* Validate: kiểm tra thiếu dữ liệu, sai kiểu, sai ngày và lỗi mapping.
* Transform: clean tên, chuẩn hóa code, cast amount và parse ngày.
* Left join: enrich customer với province nhưng vẫn giữ record nguồn.
* `valid_df`: record `OK` dùng cho report/output chính.
* `etl_summary`: bảng tóm tắt số lượng theo trạng thái xử lý.

### Nhớ nhanh

* Final project nối lại các phần: read, clean, validate, join, report, write.
* Invalid output giúp điều tra lỗi mà không làm mất dữ liệu nguồn.
