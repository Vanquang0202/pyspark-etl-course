# Chapter 21 - Incremental ETL and Watermark

## Bài này làm gì?

Bài này demo incremental ETL bằng watermark.
Thay vì đọc toàn bộ dữ liệu mỗi lần, job chỉ lấy record mới hoặc record đã thay đổi.
Ví dụ dùng cột `updated_at` và một biến `last_watermark` giả lập.

## Học được gì?

- Full load là đọc toàn bộ dữ liệu.
- Incremental load là chỉ đọc phần mới hoặc thay đổi.
- Watermark giúp biết lần trước đã xử lý đến đâu.
- Job thực tế thường lưu watermark trong bảng như `ETL_WATERMARK`.

## Khái niệm chính

`Full load`: mỗi lần chạy đọc lại toàn bộ source.

`Incremental load`: mỗi lần chạy chỉ đọc dữ liệu mới hơn watermark.

`Watermark`: mốc xử lý cuối cùng, có thể là thời gian hoặc id tăng dần.

`ETL_WATERMARK`: bảng quản lý trạng thái chạy job trong ETL thực tế.

## Lệnh chạy

```bash
python chapters/21_incremental_etl_watermark/demo.py
python chapters/21_incremental_etl_watermark/exercise.py
```

## Nhớ nhanh

- Full load đơn giản nhưng tốn tài nguyên khi dữ liệu lớn.
- Incremental load chạy nhanh hơn nhưng phải quản lý watermark cẩn thận.
- Chỉ update watermark sau khi ghi target thành công.
- Có thể dùng `updated_at` hoặc id tăng dần tùy source.
