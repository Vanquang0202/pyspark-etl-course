# Chapter 39 - Kafka Streaming Pipeline

## Ghi chú học tập

Kafka là nền tảng truyền event liên tục. Producer gửi message, consumer đọc message; topic là kênh chứa các event cùng loại. Redpanda trong compose tương thích Kafka và gọn để học local.

Checkpoint lưu vị trí xử lý của streaming query để job có thể tiếp tục sau khi restart, hạn chế đọc lại event đã xử lý.

## Chạy thử trên Ubuntu/WSL

```bash
cd chapters/39_kafka_streaming_pipeline
docker compose up -d
python producer.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 consumer_spark.py
docker compose down
```

`consumer_spark.py` in event ra console và dùng checkpoint local khi bạn chủ động chạy nó.
