# Chapter 42 - Spark Submit Deployment

## Ghi chú học tập

`python job.py` chạy Python trực tiếp và phù hợp demo nhỏ. `spark-submit job.py` là cách chuẩn để Spark nhận master, config, package và argument khi submit job.

Airflow thường trigger `spark-submit` bằng operator/task; cluster production nhận submit qua YARN, Kubernetes hoặc Spark standalone. Khác biệt chính là nơi chạy driver/executor và cách quản lý resource/config.

## Lệnh chạy

```bash
bash chapters/42_spark_submit_deployment/spark_submit_examples.sh
spark-submit --master 'local[*]' chapters/42_spark_submit_deployment/demo.py --run-date 2026-07-23
python chapters/42_spark_submit_deployment/exercise.py --run-date 2026-07-23
```

Script chỉ in các lệnh mẫu, không submit job.
