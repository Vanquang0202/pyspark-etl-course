# Chapter 01 - DataFrame Basics

## Mục tiêu

Hiểu các khái niệm đầu tiên:

- SparkSession: điểm bắt đầu của app PySpark.
- DataFrame: dữ liệu dạng bảng, gồm row và column.
- show(): xem dữ liệu.
- printSchema(): xem kiểu dữ liệu.
- count(): đếm số bản ghi.

## Chạy demo

```bash
python chapters/01_dataframe_basics/demo.py
```

## Bài tập

Sửa file `exercise.py` để tạo DataFrame employee gồm:

- employee_id
- employee_name
- department
- salary

Sau đó in data, schema và tổng số nhân viên.

## Quick Notes

### Bài này học gì?

* Làm quen với `SparkSession` và cách tạo DataFrame.
* Biết xem dữ liệu, schema và số dòng cơ bản.
* Hiểu DataFrame là nền tảng chính khi làm ETL bằng PySpark.

### Khái niệm chính

* `SparkSession`: điểm bắt đầu để làm việc với Spark.
* `DataFrame`: dữ liệu dạng bảng có dòng, cột và schema.
* `show()`: hiển thị một số dòng dữ liệu mẫu.
* `printSchema()`: xem tên cột và kiểu dữ liệu.
* `count()`: đếm số dòng và kích hoạt Spark chạy thật.

### Nhớ nhanh

* Muốn làm PySpark gần như luôn bắt đầu từ `SparkSession`.
* DataFrame là object chính để đọc, biến đổi và ghi dữ liệu.
