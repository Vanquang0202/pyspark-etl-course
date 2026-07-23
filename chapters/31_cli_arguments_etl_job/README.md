# Chapter 31 - CLI Arguments for ETL Job

## Bài này làm gì?

Bài này demo cách truyền tham số khi chạy ETL job từ command line.
Job nhận input path, output path, run date và write mode bằng `argparse`.

## Học được gì?

- Dùng `argparse` để nhận tham số chạy job.
- Truyền `input_path`, `output_path`, `run_date` và `write_mode`.
- Dùng tham số để điều khiển ETL thay vì sửa code.
- In rõ job đang đọc gì và sẽ ghi ra đâu.

## Vì sao không nên hard-code path/config?

Job ETL thực tế thường chạy nhiều lần, nhiều ngày và nhiều môi trường.
Nếu hard-code path trong code, mỗi lần đổi input, output hoặc ngày chạy lại phải sửa file Python.

Truyền tham số giúp:

- Chạy lại job theo ngày dễ hơn.
- Dùng chung code cho dev, test và prod.
- Deploy job bằng scheduler thuận tiện hơn.
- Giảm rủi ro sửa nhầm logic xử lý.

## Lệnh chạy

```bash
python chapters/31_cli_arguments_etl_job/demo.py \
  --input-path data/input/customer.csv \
  --output-path data/output/chapter31/customers \
  --run-date 2026-07-20 \
  --write-mode overwrite

python chapters/31_cli_arguments_etl_job/exercise.py \
  --input-path data/input/customer.csv \
  --output-path data/output/chapter31/exercise \
  --run-date 2026-07-20 \
  --write-mode overwrite
```

## Nhớ nhanh

- Path và run date là tham số vận hành, không phải logic ETL.
- `argparse` giúp job fail sớm nếu thiếu argument bắt buộc.
- `write_mode` nên được truyền rõ ràng để tránh ghi nhầm dữ liệu.
