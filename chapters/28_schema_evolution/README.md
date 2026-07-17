# Chapter 28 - Schema Evolution

## Bài này làm gì?

Bài này demo tình huống source data thay đổi schema.
Ví dụ source có thêm cột mới, thiếu cột cũ hoặc đổi kiểu dữ liệu.

## Học được gì?

- Nhận biết cột mới xuất hiện trong source.
- Bổ sung cột bị thiếu bằng giá trị null/default.
- Cast lại cột bị đổi kiểu dữ liệu.
- Đưa nhiều version source về cùng một schema chuẩn.

## Schema evolution là gì?

Schema evolution là việc schema dữ liệu thay đổi theo thời gian.
Trong ETL thực tế, source system có thể thêm cột, bỏ cột hoặc đổi kiểu dữ liệu mà job downstream vẫn cần chạy ổn định.

ETL cần xử lý schema evolution để:

- Tránh job fail khi source thay đổi nhỏ.
- Giữ schema target nhất quán.
- Dễ kiểm soát backward compatibility.
- Biết cột mới nào cần đưa vào target sau này.

## Lệnh chạy

```bash
python chapters/28_schema_evolution/demo.py
python chapters/28_schema_evolution/exercise.py
```

## Nhớ nhanh

- Target schema nên rõ ràng hơn source schema.
- Cột thiếu có thể thêm bằng `lit(None).cast(...)`.
- Dữ liệu đổi kiểu cần cast có kiểm soát trước khi load.
