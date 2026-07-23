# Chapter 34 - ETL Monitoring Metrics

## Bài này làm gì?

Bài này demo cách tạo metrics cho một ETL job nhỏ.
Metrics được ghi ra JSON hoặc CSV để kiểm tra sau khi job chạy.

## Học được gì?

- Tạo các chỉ số theo dõi job.
- Đếm total, valid, invalid và duplicate records.
- Ghi start time, end time và duration.
- Ghi trạng thái job thành công hoặc thất bại.
- Lưu metrics ra file nhỏ.

## Metrics trong bài

```text
total_records
valid_records
invalid_records
duplicate_records
start_time
end_time
duration_seconds
job_status
```

## Vì sao cần metrics?

Log cho biết job đang làm gì.
Metrics cho biết job chạy có ổn không.

Ví dụ nếu hôm nay `total_records` giảm mạnh hoặc `invalid_records` tăng bất thường, ta có tín hiệu để kiểm tra source data hoặc logic ETL.

## Lệnh chạy

```bash
python chapters/34_etl_monitoring_metrics/demo.py
python chapters/34_etl_monitoring_metrics/exercise.py
```

## Nhớ nhanh

- Metrics nên nhỏ, rõ và dễ đọc.
- Metrics nên được ghi dù job thành công hay thất bại.
- Dashboard và alert thường bắt đầu từ metrics đơn giản.
