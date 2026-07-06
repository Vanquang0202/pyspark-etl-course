# Chapter 10 - GroupBy Report

## Mục tiêu học

Sau chương này, bạn có thể:

- Gom nhóm dữ liệu bằng `groupBy()`.
- Tính chỉ số tổng hợp bằng `agg()`.
- Dùng `count`, `sum`, `avg`, `min`, `max`.
- Làm report theo tỉnh hoặc segment khách hàng.
- Sắp xếp kết quả report bằng `orderBy()`.

## Kiến thức chính

`groupBy()` dùng để gom các dòng có cùng key thành một nhóm. Sau đó `agg()` tính các chỉ số tổng hợp cho từng nhóm.

Ví dụ:

```python
df.groupBy("province_name").agg(
    count("*").alias("customer_count"),
    spark_sum("amount").alias("total_amount"),
)
```

Aggregation thường làm giảm số dòng so với input vì nhiều record nguồn được gom thành một dòng report.

## Liên hệ ETL thực tế

Report ETL thường là kết quả sau khi dữ liệu đã được:

- Clean kiểu dữ liệu.
- Chuẩn hóa mã danh mục.
- Join mapping để lấy tên đầy đủ.
- Tạo segment hoặc business category.

Trong chapter này, customer được join với province trước, sau đó aggregate theo `province_name` hoặc theo cặp `province_name`, `customer_segment`.

## Cách chạy trên Ubuntu/WSL

```bash
cd /mnt/e/PySpark/pyspark-etl-course
source .venv-linux/bin/activate

python chapters/10_groupby_report/demo.py
python chapters/10_groupby_report/exercise.py
```

## Output kỳ vọng

Demo show report theo tỉnh với các chỉ số:

```text
customer_count, total_amount, avg_amount, max_amount, min_amount
```

Exercise show report theo tỉnh và segment:

```text
province_name, customer_segment, customer_count, total_amount, avg_amount
```

Script cũng in:

```text
report_rows
```

## Quick Notes

### Bài này học gì?

* Gom nhóm dữ liệu để tạo report tổng hợp.
* Tính các chỉ số như count, sum, avg, min và max.
* Làm report theo tỉnh hoặc segment khách hàng.

### Khái niệm chính

* `groupBy()`: gom các dòng theo một hoặc nhiều key.
* `agg()`: tính các chỉ số tổng hợp sau khi group.
* `count()`: đếm số record trong nhóm.
* `sum()`: tính tổng giá trị trong nhóm.
* `avg()`: tính trung bình giá trị trong nhóm.
* `orderBy()`: sắp xếp kết quả report.

### Nhớ nhanh

* Aggregation thường làm số dòng output ít hơn input.
* Report nên chạy sau khi dữ liệu đã clean và mapping xong.
