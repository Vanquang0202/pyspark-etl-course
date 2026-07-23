#!/usr/bin/env bash
set -euo pipefail

cd /mnt/e/PySpark/pyspark-etl-course
source .venv-linux/bin/activate

python projects/lakehouse_full_flow/lakehouse_job.py \
  --batch-id batch_20260720_001 \
  --run-date 2026-07-20 \
  --write-mode overwrite
