# Chapter 12 - JDBC Read/Write Database

## Bài này làm gì?

Bài này demo cách Spark đọc và ghi dữ liệu với database qua JDBC.
Mình cần biết cách khai báo URL, driver, user/password và bảng cần đọc/ghi.
Nếu chưa có database hoặc driver thật thì bài này chủ yếu để hiểu cấu hình và cách viết code.
Trong thực tế có thể đọc dữ liệu từ `VSS_ODS`, xử lý bằng PySpark rồi ghi sang `VSS_360`.

## Học được gì?

- Spark đọc/ghi database bằng `.format("jdbc")`.
- JDBC cần `url`, `driver`, `user`, `password`.
- Đọc bảng bằng `dbtable` hoặc SQL bằng `query`.
- Ghi database thường dùng `append` hoặc `overwrite`.
- Code JDBC thật cần database và driver jar phù hợp.

## Khái niệm chính

`JDBC`: cách Spark kết nối với database để đọc/ghi dữ liệu.

`JDBC URL`: địa chỉ database, ví dụ host, port, service/database name.

`driver`: class driver JDBC, tùy loại database như Oracle, PostgreSQL, SQLite.

`dbtable`: tên bảng Spark sẽ đọc hoặc ghi.

`query`: câu SQL dùng khi muốn đọc dữ liệu theo logic riêng.

## Lệnh chạy

```bash
python chapters/12_jdbc_read_write_database/demo.py
python chapters/12_jdbc_read_write_database/exercise.py
```

## Nhớ nhanh

- JDBC = Spark nói chuyện với database.
- Thiếu driver/database thật thì chỉ nên xem như demo code.
- Đọc `VSS_ODS`, ghi `VSS_360` là case ETL thực tế.
- Không hard-code password trong project thật.
