# Chapter 38 - Postgres JDBC ETL Practice

## Ghi chú học tập

JDBC là chuẩn kết nối Java giữa Spark và database. Khác với đọc file, Spark cần URL, driver JDBC, user/password và tên bảng; dữ liệu được đọc/ghi qua kết nối database.

Production thường dùng JDBC khi source/target là OLTP, data mart hoặc bảng staging. Cần cân nhắc partition đọc, tải database và secret manager; không hard-code mật khẩu thật.

## Chạy thử tùy chọn

```bash
cd chapters/38_postgres_jdbc_etl_practice
docker compose up -d
export POSTGRES_PASSWORD=demo_password
spark-submit --packages org.postgresql:postgresql:42.7.5 demo.py
docker compose down
```

Postgres compose chỉ tạo database demo. Tạo bảng `public.customer_source` trước khi chạy job. Demo ghi vào `public.customer_target`.
