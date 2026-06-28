# Chapter 08 - Data Manipulation

## Mục tiêu học

Sau chương này, bạn có thể:

- Chọn cột cần dùng bằng `select()`.
- Lọc dữ liệu bằng `filter()` hoặc `where()`.
- Thêm hoặc chuẩn hóa cột bằng `withColumn()`.
- Đổi tên cột bằng `withColumnRenamed()`.
- Loại cột không cần dùng bằng `drop()`.
- Loại dòng trùng bằng `distinct()`.
- Sắp xếp dữ liệu bằng `orderBy()`.
- Tạo cột phân loại bằng `when()` và `otherwise()`.

## Kiến thức chính

Data manipulation là nhóm thao tác biến đổi dữ liệu sau khi đọc source. Trong chapter này, dữ liệu customer được chuẩn hóa qua các bước:

- Trim khoảng trắng trong tên khách hàng.
- Chuẩn hóa `province_code`.
- Cast `amount` sang kiểu số.
- Parse `birth_date` sang kiểu date.
- Tạo `birth_year` và `amount_level` hoặc `customer_segment`.
- Chỉ giữ các cột cần dùng cho output.

Các hàm text như `trim()`, `upper()`, `lower()` và `initcap()` giúp dữ liệu nhất quán hơn trước khi join hoặc làm report.

## Liên hệ ETL thực tế

Trong ETL, dữ liệu nguồn thường bị dư khoảng trắng, sai kiểu dữ liệu hoặc có format ngày tháng chưa chuẩn. Nếu không clean trước, bước join mapping hoặc aggregation phía sau có thể sai kết quả.

Chapter này là bước chuẩn bị dữ liệu trước khi qua Chapter 09 join danh mục và Chapter 10 làm report.

## Cách chạy trên Ubuntu/WSL

```bash
cd /mnt/e/PySpark/pyspark-etl-course
source .venv-linux/bin/activate

python chapters/08_data_manipulation/demo.py
python chapters/08_data_manipulation/exercise.py
```

## Output kỳ vọng

Demo hiển thị schema ban đầu, sau đó hiển thị DataFrame đã clean và schema cuối. Output có các cột như:

```text
customer_id, customer_name, gender, birth_date, amount, province_code_clean, amount_level, birth_year
```

Exercise in:

```text
input_count
output_count
```

Sau đó show DataFrame output gồm:

```text
customer_id, customer_name, province_code, amount, customer_segment, processing_date
```

## Quick Notes

### Bài này học gì?

* Clean và chuẩn hóa dữ liệu sau khi đọc source.
* Chọn, lọc, thêm, đổi tên và loại cột trong DataFrame.
* Tạo cột phân loại phục vụ bước join/report phía sau.

### Khái niệm chính

* `select()`: chọn các cột cần giữ.
* `filter()/where()`: lọc dòng theo điều kiện.
* `withColumn()`: thêm hoặc thay đổi giá trị một cột.
* `withColumnRenamed()`: đổi tên cột.
* `drop()`: loại cột không cần dùng.
* `when()/otherwise()`: tạo logic phân loại theo điều kiện.

### Nhớ nhanh

* Clean dữ liệu trước giúp join và report chính xác hơn.
* Data manipulation là bước transform cốt lõi trong ETL.
