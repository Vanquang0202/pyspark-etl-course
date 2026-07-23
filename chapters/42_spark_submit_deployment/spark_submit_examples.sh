#!/usr/bin/env bash
# Chỉ in lệnh mẫu, không chạy spark-submit.

echo "spark-submit chapters/42_spark_submit_deployment/demo.py --run-date 2026-07-23"
echo "spark-submit --master 'local[*]' chapters/42_spark_submit_deployment/demo.py --run-date 2026-07-23"
echo "spark-submit --master 'local[*]' --conf spark.sql.shuffle.partitions=4 chapters/42_spark_submit_deployment/demo.py --run-date 2026-07-23 --environment local"
