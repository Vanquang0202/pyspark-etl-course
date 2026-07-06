# Chapter 11 - Spark SQL

## Mục tiêu học

Sau chương này, bạn có thể:

- Tạo temporary view bằng `createOrReplaceTempView()`.
- Query DataFrame bằng `spark.sql()`.
- Viết SQL `SELECT`, `WHERE`, `JOIN`, `CASE WHEN`, `GROUP BY`.
- So sánh cách dùng DataFrame API và Spark SQL.
- Kết hợp SQL với pipeline PySpark khi logic nghiệp vụ dễ viết bằng SQL.

## Kiến thức chính

Spark SQL không phải là một database riêng. Đây là cách viết query SQL chạy trên dữ liệu đang được quản lý bởi `SparkSession`.

Temporary view giống bảng tạm:

```python
customer_df.createOrReplaceTempView("customers")
```

Sau đó có thể query:

```python
spark.sql("SELECT * FROM customers")
```

Temp view chỉ tồn tại trong SparkSession hiện tại. Khi session dừng, view cũng mất.

## Liên hệ ETL thực tế

Nhiều team dữ liệu dùng SQL để business logic dễ đọc hơn, nhất là các rule mapping, phân loại segment và report. PySpark cho phép dùng DataFrame API và Spark SQL thay thế hoặc kết hợp với nhau.

Ví dụ:

- DataFrame API để đọc file và chuẩn hóa schema.
- Spark SQL để viết logic join, `CASE WHEN`, aggregate.
- DataFrame API để ghi output hoặc tiếp tục transform.

## Cách chạy trên Ubuntu/WSL

```bash
cd /mnt/e/PySpark/pyspark-etl-course
source .venv-linux/bin/activate

python chapters/11_spark_sql/demo.py
python chapters/11_spark_sql/exercise.py
```

## Output kỳ vọng

Demo show các kết quả chính:

- SQL `SELECT` cơ bản.
- SQL `WHERE amount > 0`.
- SQL `JOIN` customer với province.
- SQL `CASE WHEN` tạo `customer_segment`.
- SQL `GROUP BY province_name`.

Exercise in:

```text
report_rows
```

Sau đó show SQL report gồm:

```text
province_name, customer_segment, customer_count, total_amount, avg_amount
```

## Quick Notes

### Bài này học gì?

* Query DataFrame bằng Spark SQL.
* Tạo temp view để viết logic bằng SQL.
* Kết hợp SQL với DataFrame API trong pipeline ETL.

### Khái niệm chính

* `createOrReplaceTempView()`: tạo bảng tạm từ DataFrame.
* `spark.sql()`: chạy câu SQL trong SparkSession.
* `SELECT/WHERE`: chọn và lọc dữ liệu bằng SQL.
* `JOIN`: nối dữ liệu bằng cú pháp SQL.
* `CASE WHEN`: tạo logic phân loại trong SQL.
* `GROUP BY`: gom nhóm và tính report bằng SQL.

### Nhớ nhanh

* Temp view chỉ tồn tại trong SparkSession hiện tại.
* Spark SQL phù hợp khi business logic dễ đọc hơn bằng SQL.
