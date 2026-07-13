# Chapter 23 - Streaming ETL with Checkpoint

## Bài này làm gì?

Bài này demo streaming ETL có checkpoint.
Source dùng `rate` nên không cần Kafka hoặc file mới.
Stream đọc dữ liệu liên tục, transform đơn giản rồi ghi ra console.

## Học được gì?

- Dùng `readStream` để đọc stream.
- Transform streaming DataFrame giống batch DataFrame.
- Dùng `writeStream` để ghi stream.
- Thêm `checkpointLocation`.
- Dùng `trigger` để cấu hình nhịp xử lý.

## Khái niệm chính

`Checkpoint`: nơi Spark lưu tiến độ và metadata của streaming query.

`Trigger`: cấu hình khi nào Spark chạy micro-batch tiếp theo.

`Streaming job`: job thường chạy liên tục, không kết thúc ngay như batch.

`Rate source`: source giả lập sinh dòng mới theo thời gian, tiện học streaming local.

## Lệnh chạy

```bash
python chapters/23_streaming_etl_with_checkpoint/demo.py
python chapters/23_streaming_etl_with_checkpoint/exercise.py
```

Trên local, nếu query chạy liên tục thì dừng bằng `Ctrl + C`.

## Nhớ nhanh

- Checkpoint giúp Spark nhớ tiến độ.
- Mất checkpoint có thể làm stream xử lý lại từ đầu.
- Streaming job thường chạy liên tục.
- Bài này ghi console, checkpoint path chỉ để học concept.
