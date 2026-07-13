# Chapter 18 - Spark Performance Basics

## Bài này làm gì?

Bài này ghi chú các ý cơ bản khi bắt đầu tối ưu Spark.
Mục tiêu là hiểu Spark chạy theo plan, không chạy ngay ở từng dòng code.
Khi gặp action, Spark mới thật sự tạo job để xử lý dữ liệu.

## Học được gì?

- Lazy execution trong Spark.
- Transformation khác action như thế nào.
- Vì sao `show`, `count`, `limit` thường kích hoạt job.
- Dùng `explain()` để xem kế hoạch chạy.
- Shuffle là điểm thường tốn tài nguyên.

## Khái niệm chính

`Transformation`: tạo DataFrame mới, ví dụ `select`, `filter`, `withColumn`, `groupBy`.

`Action`: bắt Spark chạy thật, ví dụ `show`, `count`, `collect`, `write`.

`Lazy execution`: Spark chỉ dựng kế hoạch trước, chưa xử lý dữ liệu ngay.

`Explain plan`: cách xem Spark dự định đọc, lọc, join, aggregate dữ liệu như thế nào.

`Shuffle`: Spark phải di chuyển dữ liệu giữa partition, thường xảy ra khi `groupBy`, `join`, `distinct`.

## Lệnh chạy

```bash
python chapters/18_spark_performance_basics/demo.py
python chapters/18_spark_performance_basics/exercise.py
```

## Nhớ nhanh

- Transform chưa chạy ngay.
- Action mới kích hoạt Spark job.
- `explain()` giúp nhìn plan trước khi chạy nặng.
- Shuffle càng nhiều thì job càng dễ chậm.
