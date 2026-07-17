# Chapter 29 - Upsert and SCD Concept

## Bài này làm gì?

Bài này demo concept upsert và Slowly Changing Dimension bằng DataFrame.
Không cần database thật, chỉ mô phỏng old target data và new source data.

## Học được gì?

- Detect record cần insert.
- Detect record cần update.
- Tạo final target giả lập sau upsert.
- Hiểu SCD Type 1 ở mức concept.
- Liên hệ flow ghi dữ liệu từ ODS sang 360.

## Khái niệm chính

`Insert`: thêm record mới chưa có trong target.

`Update`: cập nhật record đã có trong target khi dữ liệu nguồn thay đổi.

`Upsert`: nếu key chưa có thì insert, nếu key đã có thì update.

`SCD Type 1`: cập nhật đè giá trị mới lên giá trị cũ, không giữ lịch sử thay đổi.

## Liên hệ ODS sang 360

ODS thường giữ dữ liệu gần source.
Bảng 360 thường cần dữ liệu customer mới nhất để phục vụ báo cáo hoặc tra cứu.

Khi load từ ODS sang 360:

- Customer mới thì insert vào 360.
- Customer đã tồn tại nhưng thay đổi tên, tỉnh hoặc trạng thái thì update.
- Nếu dùng SCD Type 1, bảng 360 chỉ giữ phiên bản mới nhất.

## Lệnh chạy

```bash
python chapters/29_upsert_scd_concept/demo.py
python chapters/29_upsert_scd_concept/exercise.py
```

## Nhớ nhanh

- Upsert luôn cần key rõ ràng.
- SCD Type 1 đơn giản nhưng không giữ lịch sử.
- Muốn giữ lịch sử thì cần học tiếp SCD Type 2.
