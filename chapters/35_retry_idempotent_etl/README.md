# Chapter 35 - Retry and Idempotent ETL

## Bài này làm gì?

Bài này demo retry và idempotent trong ETL.
Job giả lập lỗi tạm thời, retry lại và ghi output theo `run_date`.

## Học được gì?

- Retry một function có thể lỗi tạm thời.
- Giới hạn số lần retry.
- Dùng `run_id` để theo dõi lần chạy.
- Ghi output theo `run_date` để chạy lại an toàn.
- Tránh duplicate output khi job chạy lại.

## Retry là gì?

Retry là chạy lại một bước khi lỗi có khả năng chỉ là tạm thời.
Ví dụ database timeout, network chập chờn hoặc API trả lỗi ngắn hạn.

Retry không nên chạy vô hạn.
Nên có số lần thử tối đa và log rõ lần thử hiện tại.

## Idempotent là gì?

Idempotent nghĩa là chạy lại cùng một job với cùng input thì không tạo kết quả sai hoặc duplicate.
Trong ETL batch, cách đơn giản là ghi output theo `run_date` và dùng `overwrite` cho partition/ngày đó.

## Vì sao ETL job cần chạy lại an toàn?

Job production có thể fail giữa chừng.
Nếu chạy lại mà append duplicate dữ liệu, report và bảng target sẽ sai.
Idempotent giúp retry, rerun và backfill an toàn hơn.

## Lệnh chạy

```bash
python chapters/35_retry_idempotent_etl/demo.py
python chapters/35_retry_idempotent_etl/exercise.py
```

## Nhớ nhanh

- Retry xử lý lỗi tạm thời.
- Idempotent xử lý việc chạy lại job an toàn.
- Với batch ETL, `overwrite` theo `run_date` thường dễ kiểm soát hơn append toàn bộ.
