# Chapter 22 - Kafka Streaming Concept

## Bài này làm gì?

Bài này chạy Kafka local bằng Redpanda và dùng Spark Structured Streaming đọc topic thật.
Producer Python gửi JSON vào topic `customer-events`.
Spark consumer đọc message, parse JSON, cast `amount` và ghi kết quả ra console.

## Kafka topic là gì?

`Topic` là nơi chứa luồng message trong Kafka.
Ví dụ bài này dùng topic:

```text
customer-events
```

## Producer là gì?

`Producer` là chương trình gửi message vào Kafka topic.
File `producer.py` gửi khoảng 15 customer event dạng JSON.

## Consumer/Spark streaming là gì?

`Consumer` là chương trình đọc message từ topic.
Trong bài này, Spark Structured Streaming đóng vai trò consumer.
Spark đọc Kafka bằng `readStream`, transform dữ liệu rồi `writeStream` ra console.

## Offset là gì?

`Offset` là vị trí của message trong Kafka partition.
Spark dùng offset để biết đã đọc tới đâu.
Checkpoint giúp Spark lưu lại tiến độ này.

## Cài thêm thư viện producer

Nếu thiếu `kafka-python`, cài trong virtual environment:

```bash
python -m pip install kafka-python
```

Spark Kafka package được khai báo trực tiếp trong code:

```text
org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1
```

Lần đầu chạy Spark Kafka có thể mất vài phút để tải JAR.
Nếu Maven Central bị `connection reset`, bài này đã cấu hình thêm mirror `repo.maven.apache.org` và Aliyun public mirror.
Nếu vẫn lỗi, xóa Ivy cache rồi chạy lại:

```bash
rm -rf ~/.ivy2/cache/org.apache.spark/spark-sql-kafka-0-10_2.13
rm -rf ~/.ivy2/jars/org.apache.spark_spark-sql-kafka-0-10_2.13-4.0.1.jar
```

## Lệnh chạy Kafka local

```bash
cd /mnt/e/PySpark/pyspark-etl-course
source .venv-linux/bin/activate

cd chapters/22_kafka_streaming_concept
docker compose up -d
```

Compose có service nhỏ để tạo sẵn topic `customer-events`.

## Lệnh tạo topic

Redpanda thường tự tạo topic khi producer gửi message.
Nếu muốn tạo thủ công:

```bash
docker compose exec redpanda rpk topic create customer-events
docker compose exec redpanda rpk topic list
```

## Lệnh chạy Spark consumer

Mở terminal 1:

```bash
cd /mnt/e/PySpark/pyspark-etl-course
source .venv-linux/bin/activate
python chapters/22_kafka_streaming_concept/exercise.py
```

`demo.py` cũng trỏ tới cùng consumer Kafka thật:

```bash
python chapters/22_kafka_streaming_concept/demo.py
```

## Lệnh chạy producer

Mở terminal 2:

```bash
cd /mnt/e/PySpark/pyspark-etl-course
source .venv-linux/bin/activate
cd chapters/22_kafka_streaming_concept
python producer.py
```

## Cách dừng

Streaming job có thể chạy liên tục.
Dừng Spark consumer bằng `Ctrl + C`.
Dừng Kafka local:

```bash
cd /mnt/e/PySpark/pyspark-etl-course/chapters/22_kafka_streaming_concept
docker compose down
```

## Nhớ nhanh

- Producer gửi JSON vào topic.
- Spark đọc topic bằng Structured Streaming.
- `startingOffsets=earliest` giúp đọc từ message cũ khi mới chạy.
- Checkpoint nằm ở `data/output/chapter_22/checkpoint`.
- Nên chạy trên Ubuntu/WSL; nếu chạy PowerShell Windows và gặp lỗi Hadoop/winutils thì chuyển sang WSL.
