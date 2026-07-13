# Chapter 20 - Database ETL Practical Flow

## Bài này làm gì?

Bài này mô phỏng flow ETL đọc từ database source và ghi sang database target.
Không cần database thật, JDBC driver thật hoặc password thật.
Code tạo DataFrame giả lập như một bảng source rồi transform sang schema target.

## Học được gì?

- Source DB có thể xem như `VSS_ODS`.
- Target DB có thể xem như `VSS_360`.
- JDBC URL, driver, user, password chỉ là config.
- Job thực tế thường đọc source, clean, validate, mapping rồi ghi target.

## Khái niệm chính

`Source table`: bảng dữ liệu đầu vào từ hệ thống nguồn.

`Target schema`: cấu trúc dữ liệu sau khi xử lý để ghi sang hệ thống đích.

`JDBC`: cách Spark đọc/ghi database quan hệ như SQL Server, Postgres, Oracle.

`Validate`: kiểm tra record có đủ điều kiện ghi sang target không.

## Lệnh chạy

```bash
python chapters/20_database_etl_practical_flow/demo.py
python chapters/20_database_etl_practical_flow/exercise.py
```

## Nhớ nhanh

- Bài này chỉ mô phỏng DB, chưa kết nối DB thật.
- Khi làm thật cần JDBC driver, URL, user và password.
- Không hard-code password trong code production.
- Nên tách rõ bước đọc source, transform và ghi target.
