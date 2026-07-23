# Chapter 33 - Data Contract and Schema Validation

## Bài này làm gì?

Bài này demo data contract và validate schema input trước khi transform.
Job so sánh schema thực tế với schema kỳ vọng.

## Học được gì?

- Khai báo expected schema.
- Lấy actual schema từ DataFrame.
- Check thiếu cột.
- Check sai kiểu dữ liệu.
- Báo lỗi rõ ràng khi schema không đúng.

## Vì sao cần data contract?

Source data có thể thay đổi mà team ETL không biết trước.
Ví dụ source đổi `amount` từ double sang string, hoặc bỏ cột `province_code`.
Nếu job vẫn chạy tiếp, lỗi có thể xuất hiện muộn hơn hoặc tạo output sai.

Data contract giúp job fail sớm và báo đúng vấn đề:

- Cột nào bị thiếu.
- Cột nào sai kiểu.
- Schema hiện tại đang là gì.

## Lệnh chạy

```bash
python chapters/33_data_contract_schema_validation/demo.py
python chapters/33_data_contract_schema_validation/exercise.py
```

## Nhớ nhanh

- Validate schema nên chạy ngay sau bước đọc input.
- Lỗi schema nên rõ ràng để source team và ETL team cùng xử lý.
- Contract không thay thế data quality rule, mà bổ sung lớp bảo vệ đầu vào.
