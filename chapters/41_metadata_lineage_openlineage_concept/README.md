# Chapter 41 - Metadata và Lineage Concept

## Ghi chú học tập

Metadata là dữ liệu mô tả dữ liệu, ví dụ schema, owner, location. Lineage mô tả dữ liệu đi từ đâu đến đâu và được xử lý bởi job nào.

Job lineage theo dõi quan hệ input/output của một job. Field lineage chi tiết hơn: cột output được tạo từ cột input nào. OpenLineage thường mô hình hóa `job`, `run`, `input`, `output` và event trạng thái.

Airflow có thể gửi run context, Spark phát event xử lý, còn Metadata Hub lưu catalog và lineage để tra cứu. Chapter này chỉ mô phỏng JSON, không cài OpenLineage thật.

## Lệnh chạy

```bash
python chapters/41_metadata_lineage_openlineage_concept/demo.py
python chapters/41_metadata_lineage_openlineage_concept/exercise.py
```
