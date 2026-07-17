# Chapter 25 - Logging and Error Handling

## Bài này làm gì?

Bài này demo logging trong một job ETL nhỏ.
Job ghi log lúc bắt đầu, kết thúc, số record input/output và lỗi validate.

## Học được gì?

- Dùng module `logging` của Python trong PySpark job.
- Ghi log start/end job.
- Ghi số lượng record input, valid và invalid.
- Dùng `try/except` để bắt lỗi cơ bản.
- Raise lỗi khi validate không đạt ngưỡng mong muốn.

## Vì sao job ETL cần log?

Job ETL thường chạy tự động theo lịch.
Khi job lỗi, log là nơi đầu tiên giúp biết job đang đọc gì, xử lý bao nhiêu record và fail ở bước nào.

Log tốt giúp:

- Điều tra lỗi nhanh hơn.
- Biết dữ liệu hôm nay tăng hay giảm bất thường.
- Theo dõi số record bị reject.
- Dễ vận hành job trên dev, test và prod.

## Lệnh chạy

```bash
python chapters/25_logging_error_handling/demo.py
python chapters/25_logging_error_handling/exercise.py
```

## Nhớ nhanh

- Log nên có thời gian, level và message rõ ràng.
- Không nuốt lỗi âm thầm trong `except`.
- Khi bắt lỗi, nên log bằng `logger.exception()` để thấy stack trace.
