# Chapter 27 - Advanced Data Quality Rules

## Bài này làm gì?

Bài này demo các rule data quality nâng cao hơn so với kiểm tra null cơ bản.
Dữ liệu được tách thành valid records và invalid records để xử lý tiếp.

## Học được gì?

- Check required field.
- Check `amount > 0`.
- Check `province_code` có nằm trong danh sách hợp lệ.
- Phát hiện duplicate `customer_id`.
- Phát hiện date sai format.
- Tạo `valid_df` và `invalid_df`.

## Data quality trong ETL thực tế

Data quality giúp bảo vệ bảng đích khỏi dữ liệu sai.
Trong hệ thống thật, record lỗi thường không bị xóa ngay mà được ghi vào reject zone hoặc error table để điều tra.

Rule nên rõ ràng, dễ đọc và có mã lỗi cụ thể.
Khi có nhiều lỗi trên một record, job có thể lấy lỗi đầu tiên hoặc lưu danh sách lỗi tùy yêu cầu.

## Lệnh chạy

```bash
python chapters/27_advanced_data_quality_rules/demo.py
python chapters/27_advanced_data_quality_rules/exercise.py
```

## Nhớ nhanh

- Required field là rule nền tảng nhất.
- Duplicate key thường cần window function hoặc groupBy.
- Valid records đi tiếp, invalid records để điều tra.
