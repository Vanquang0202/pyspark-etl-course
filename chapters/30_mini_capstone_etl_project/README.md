# Chapter 30 - Mini Capstone ETL Project

## Bài này làm gì?

Bài này tổng hợp các kiến thức đã học thành một ETL nhỏ.
Flow đọc raw customer data, join province mapping, kiểm tra data quality, tách valid/invalid và tạo summary report.

## Flow xử lý

```text
Đọc raw customer data
-> Đọc province mapping
-> Clean / cast
-> Apply data quality rules
-> Join mapping
-> Detect duplicate
-> Tách valid / invalid
-> Tạo summary report
-> In output preview
```

## Học được gì?

- Đọc nhiều input.
- Clean string và cast amount/date.
- Validate required field, amount, date và province mapping.
- Detect duplicate customer.
- Tách valid và invalid records.
- Tạo summary report theo trạng thái dữ liệu.

## Vì sao là bài tổng hợp?

Chapter này nối lại các phần quan trọng của course:

- Read file.
- DataFrame transformation.
- Casting và data quality.
- Join mapping.
- GroupBy report.
- Logging tư duy output valid/invalid.

Đây là bài nhỏ để ôn lại trước khi chuyển sang final project hoặc ETL production phức tạp hơn.

## Lệnh chạy

```bash
python chapters/30_mini_capstone_etl_project/demo.py
python chapters/30_mini_capstone_etl_project/exercise.py
```

## Nhớ nhanh

- ETL thực tế nên giữ cả raw value và clean value khi cần điều tra lỗi.
- Invalid records không nên bị mất.
- Summary report giúp kiểm tra nhanh chất lượng mỗi lần chạy.
