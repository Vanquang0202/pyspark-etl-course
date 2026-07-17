# Chapter 26 - Config Driven ETL

## Bài này làm gì?

Bài này demo cách tách config ra khỏi code.
Thay vì hard-code input path, output path, run date và write mode trong job, ta đọc các giá trị này từ file JSON.

## Học được gì?

- Tạo file config mẫu dạng JSON.
- Đọc config bằng Python `json`.
- Dùng `input_path`, `output_path`, `run_date` và `write_mode` trong ETL job.
- Hiểu cách đổi config cho dev, test và prod.

## Vì sao cần config-driven ETL?

Cùng một logic ETL có thể chạy ở nhiều môi trường.
Nếu hard-code path hoặc mode trong code, mỗi lần đổi môi trường lại phải sửa code.

Config-driven ETL giúp:

- Code ổn định hơn.
- Dễ đổi input/output theo môi trường.
- Dễ chạy lại job theo `run_date`.
- Dễ kiểm soát `append`, `overwrite` hoặc các mode ghi khác.

## Lệnh chạy

```bash
python chapters/26_config_driven_etl/demo.py
python chapters/26_config_driven_etl/exercise.py
```

## Nhớ nhanh

- Config là tham số chạy job, không phải logic xử lý.
- Dev/test/prod nên khác config, không khác code.
- Không nên để password thật trong config commit lên Git.
