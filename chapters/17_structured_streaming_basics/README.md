# Chapter 17 - Structured Streaming Basics

## Bài này làm gì?

Bài này demo Structured Streaming cơ bản trong PySpark.
Structured Streaming dùng để xử lý dữ liệu đến liên tục.
Demo đọc stream đơn giản rồi ghi ra console hoặc output.
Streaming job có thể chạy liên tục, muốn dừng thì dùng `Ctrl + C`.

## Học được gì?

- Dùng `readStream` để đọc dữ liệu streaming.
- Dùng `writeStream` để ghi kết quả streaming.
- Hiểu `output mode` như `append`, `update`, `complete`.
- Biết checkpoint dùng để lưu tiến độ xử lý.
- Streaming thường chạy theo micro-batch.

## Khái niệm chính

`Structured Streaming`: API streaming của Spark, dùng gần giống DataFrame.

`readStream`: tạo streaming DataFrame từ source như file, rate, Kafka.

`writeStream`: ghi stream ra sink như console, file hoặc database.

`Output mode`: cách Spark ghi kết quả mới hoặc kết quả đã cập nhật.

`Checkpoint`: nơi Spark lưu metadata và tiến độ để có thể chạy tiếp.

`Micro-batch`: Spark xử lý stream thành nhiều batch nhỏ liên tục.

## Lệnh chạy

```bash
python chapters/17_structured_streaming_basics/demo.py
python chapters/17_structured_streaming_basics/exercise.py
```

Trên Windows, nếu chưa setup Hadoop/winutils thì các bài có ghi output hoặc checkpoint có thể lỗi. Nên chạy bài này bằng Ubuntu/WSL.

## Nhớ nhanh

- Streaming là xử lý dữ liệu mới đến liên tục.
- `readStream` để đọc, `writeStream` để ghi.
- Checkpoint rất quan trọng khi stream ghi output/state.
- Job chạy lâu thì dừng bằng `Ctrl + C`.
