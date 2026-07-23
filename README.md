# PySpark ETL Course - Spark 4.0.1

Project chia bai hoc PySpark thanh tung chapter de de doc, chay, lam exercise va commit Git. Chapter 01 la DataFrame basics da hoan thanh. Chapter 02-06 bam theo PySpark Tour of Types va Python Package Management; Chapter 07-11 la cac ky nang ETL thuc hanh; Chapter 12-36 la cac chu de nang cao de mo rong ETL thuc te va production hon.

## Cau truc project

```text
pyspark-etl-course/
|-- data/
|   |-- input/
|   |   |-- chapter30_customer_raw.csv
|   |   |-- customer.csv
|   |   |-- customer_transactions.csv
|   |   |-- final_project/customer_raw.csv
|   |   |-- province.csv
|   |   |-- streaming_transactions/customer_transactions.csv
|   |   |-- customer_profiles.json
|   |   `-- customer_profiles.xml
|   `-- output/                     # duoc tao khi job ghi ket qua
|-- chapters/
|   |-- 01_dataframe_basics/
|   |-- 02_basic_data_types_and_schemas/
|   |-- 03_complex_data_types/
|   |-- 04_casting_and_data_quality/
|   |-- 05_semi_structured_data/
|   |-- 06_python_package_management/
|   |-- 07_read_write_files/
|   |-- 08_data_manipulation/
|   |-- 09_join_mapping_check/
|   |-- 10_groupby_report/
|   |-- 11_spark_sql/
|   |-- 12_jdbc_read_write_database/
|   |-- 13_window_functions/
|   |-- 14_broadcast_join/
|   |-- 15_partition_repartition_coalesce/
|   |-- 16_cache_persist/
|   |-- 17_structured_streaming_basics/
|   |-- 18_spark_performance_basics/
|   |-- 19_handling_larger_data/
|   |-- 20_database_etl_practical_flow/
|   |-- 21_incremental_etl_watermark/
|   |-- 22_kafka_streaming_concept/
|   |-- 23_streaming_etl_with_checkpoint/
|   |-- 24_pyspark_unit_testing/
|   |-- 25_logging_error_handling/
|   |-- 26_config_driven_etl/
|   |-- 27_advanced_data_quality_rules/
|   |-- 28_schema_evolution/
|   |-- 29_upsert_scd_concept/
|   |-- 30_mini_capstone_etl_project/
|   |-- 31_cli_arguments_etl_job/
|   |-- 32_environment_config_dev_test_prod/
|   |-- 33_data_contract_schema_validation/
|   |-- 34_etl_monitoring_metrics/
|   |-- 35_retry_idempotent_etl/
|   |-- 36_packaging_pyspark_project/
|   |-- 37_fastapi_serving_layer/
|   |-- 38_postgres_jdbc_etl_practice/
|   |-- 39_kafka_streaming_pipeline/
|   |-- 40_data_generator_for_etl_testing/
|   |-- 41_metadata_lineage_openlineage_concept/
|   `-- 42_spark_submit_deployment/
|-- shared/
|   |-- spark_utils.py
|   `-- path_utils.py
|-- final_project/
|-- projects/
|   `-- lakehouse_full_flow/
|-- orchestration/
|   `-- airflow_local/
|-- tests/
|-- requirements.txt
`-- README.md
```

Moi chapter co:

- `README.md`: muc tieu, kien thuc va bai tap.
- `demo.py`: vi du minh hoa.
- `exercise.py`: bai thuc hanh.

## Noi dung cac chapter

| Chapter | Noi dung |
|---|---|
| 01 DataFrame Basics | SparkSession, DataFrame, show, schema, select, filter, temp view, SQL |
| 02 Basic Types and Schemas | Basic types, explicit schema, nullable, Float/Double/Decimal |
| 03 Complex Data Types | ArrayType, nested StructType, MapType |
| 04 Casting and Data Quality | cast, date parsing, null do cast loi, ANSI mode |
| 05 Semi-Structured Data | JSON, from_json, to_json, XML va VARIANT cua Spark 4 |
| 06 Python Package Management | shared code, venv, py-files, addPyFile, distribute code |
| 07 Read/Write Files | Doc CSV va ghi/kiem tra Parquet, CSV |
| 08 Data Manipulation | select, filter, alias, upper, orderBy |
| 09 Join and Mapping Check | left join va left_anti join |
| 10 GroupBy and Report | groupBy, count, sum va report |
| 11 Spark SQL | temp view, SQL join, filter va order |
| 12 JDBC Read/Write Database | Config JDBC, read/write database, partition read, lien he VSS_ODS/VSS_360 |
| 13 Window Functions | row_number, rank, dense_rank, lag, lead, sum over partition |
| 14 Broadcast Join | Join bang lon voi bang danh muc nho, broadcast hint va explain plan |
| 15 Partition, Repartition, Coalesce | Partition, repartition, coalesce, write partitionBy va part-* files |
| 16 Cache and Persist | Lazy execution, action, recompute, cache va unpersist |
| 17 Structured Streaming Basics | readStream, writeStream console, trigger, checkpoint va micro-batch |
| 18 Spark Performance Basics | Lazy execution, transformation/action, explain plan va shuffle |
| 19 Handling Larger Data | Tao du lieu lon bang range, filter/select som va kiem soat partition |
| 20 Database ETL Practical Flow | Mo phong flow doc source DB, transform, validate va ghi target DB |
| 21 Incremental ETL and Watermark | Full load, incremental load, last_watermark va new_watermark |
| 22 Kafka Streaming Concept | Topic, event, key/value, offset, checkpoint va config Kafka mau |
| 23 Streaming ETL with Checkpoint | readStream, transform, writeStream, trigger va checkpointLocation |
| 24 PySpark Unit Testing | Test logic ETL bang pytest, SparkSession fixture, clean/cast/validate |
| 25 Logging and Error Handling | Logging start/end job, record counts, validation errors va try/except |
| 26 Config Driven ETL | Tach input_path, output_path, run_date va write_mode ra config JSON |
| 27 Advanced Data Quality Rules | Required fields, valid amount, province code, duplicate va date format |
| 28 Schema Evolution | Xu ly source them cot, thieu cot va doi kieu du lieu |
| 29 Upsert and SCD Concept | Mo phong insert/update/upsert va SCD Type 1 bang DataFrame |
| 30 Mini Capstone ETL Project | Tong hop read, clean, quality rules, join mapping, duplicate va summary |
| 31 CLI Arguments for ETL Job | Truyen input_path, output_path, run_date va write_mode bang argparse |
| 32 Environment Config Dev Test Prod | Tach config JSON theo moi truong dev, test va prod |
| 33 Data Contract and Schema Validation | Khai bao expected schema va bao loi khi input thieu cot hoac sai kieu |
| 34 ETL Monitoring Metrics | Tao metrics total, valid, invalid, duplicate, duration va job_status |
| 35 Retry and Idempotent ETL | Retry loi tam thoi va ghi output theo run_date de chay lai an toan |
| 36 Packaging PySpark Project | Tach transform thanh module rieng va main.py dieu phoi job |
| 37 API Serving Layer | Mô phỏng API đọc dữ liệu sau ETL bằng HTTP server đơn giản |
| 38 Postgres JDBC ETL Practice | Đọc, clean và ghi bảng Postgres qua JDBC |
| 39 Kafka Streaming Pipeline | Producer, topic, Spark Structured Streaming consumer và checkpoint |
| 40 Data Generator for ETL Testing | Tạo customer event valid/invalid để test ETL |
| 41 Metadata and Lineage Concept | Mô phỏng lineage event theo job/run/input/output |
| 42 Spark Submit Deployment | Cách submit Spark job với master, config và argument |

## Projects

- `projects/lakehouse_full_flow` - mo phong full flow Data Lakehouse ETL local bang PySpark.

## Orchestration

- `orchestration/airflow_local` - Airflow local dung de quan ly va trigger lakehouse full flow job.
- Co nhieu DAG demo de hoc full flow, data quality, monitoring va cleanup output local.

## Moi truong Ubuntu/WSL

Project nay uu tien chay tren Ubuntu/WSL. Thu muc Windows:

```text
E:\PySpark\pyspark-etl-course
```

tuong ung voi path trong WSL:

```text
/mnt/e/PySpark/pyspark-etl-course
```

Tao virtual environment Linux va cai dependency:

```bash
cd /mnt/e/PySpark/pyspark-etl-course
python3 -m venv .venv-linux
source .venv-linux/bin/activate
python -m pip install -r requirements.txt
```

Kiem tra interpreter:

```bash
python -c "import sys; print(sys.executable)"
```

Ket qua nen tro vao:

```text
/mnt/e/PySpark/pyspark-etl-course/.venv-linux/bin/python
```

`shared/spark_utils.py` se set `PYSPARK_PYTHON` va `PYSPARK_DRIVER_PYTHON` bang interpreter dang active, de driver va worker dung cung Python.

> Ghi chu: Project nay khong can xu ly `winutils.exe` neu chay tren Ubuntu/WSL. Cac output CSV/Parquet nen duoc ghi tu moi truong WSL de tranh loi Hadoop native utility tren Windows.

## Thu tu chay

Chay demo truoc, sau do exercise cua cung chapter:

```bash
cd /mnt/e/PySpark/pyspark-etl-course
source .venv-linux/bin/activate

python chapters/01_dataframe_basics/demo.py
python chapters/01_dataframe_basics/exercise.py
python chapters/02_basic_data_types_and_schemas/demo.py
python chapters/02_basic_data_types_and_schemas/exercise.py
python chapters/03_complex_data_types/demo.py
python chapters/03_complex_data_types/exercise.py
python chapters/04_casting_and_data_quality/demo.py
python chapters/04_casting_and_data_quality/exercise.py
python chapters/05_semi_structured_data/demo.py
python chapters/05_semi_structured_data/exercise.py
python chapters/06_python_package_management/demo.py
python chapters/06_python_package_management/exercise.py
python chapters/07_read_write_files/demo.py
python chapters/07_read_write_files/exercise.py
python chapters/08_data_manipulation/demo.py
python chapters/08_data_manipulation/exercise.py
python chapters/09_join_mapping_check/demo.py
python chapters/09_join_mapping_check/exercise.py
python chapters/10_groupby_report/demo.py
python chapters/10_groupby_report/exercise.py
python chapters/11_spark_sql/demo.py
python chapters/11_spark_sql/exercise.py
python chapters/12_jdbc_read_write_database/demo.py
python chapters/12_jdbc_read_write_database/exercise.py
python chapters/13_window_functions/demo.py
python chapters/13_window_functions/exercise.py
python chapters/14_broadcast_join/demo.py
python chapters/14_broadcast_join/exercise.py
python chapters/15_partition_repartition_coalesce/demo.py
python chapters/15_partition_repartition_coalesce/exercise.py
python chapters/16_cache_persist/demo.py
python chapters/16_cache_persist/exercise.py
python chapters/17_structured_streaming_basics/demo.py
python chapters/17_structured_streaming_basics/exercise.py
python chapters/18_spark_performance_basics/demo.py
python chapters/18_spark_performance_basics/exercise.py
python chapters/19_handling_larger_data/demo.py
python chapters/19_handling_larger_data/exercise.py
python chapters/20_database_etl_practical_flow/demo.py
python chapters/20_database_etl_practical_flow/exercise.py
python chapters/21_incremental_etl_watermark/demo.py
python chapters/21_incremental_etl_watermark/exercise.py
python chapters/22_kafka_streaming_concept/demo.py
python chapters/22_kafka_streaming_concept/exercise.py
python chapters/23_streaming_etl_with_checkpoint/demo.py
python chapters/23_streaming_etl_with_checkpoint/exercise.py
pytest chapters/24_pyspark_unit_testing/demo.py
pytest chapters/24_pyspark_unit_testing/exercise.py
python chapters/25_logging_error_handling/demo.py
python chapters/25_logging_error_handling/exercise.py
python chapters/26_config_driven_etl/demo.py
python chapters/26_config_driven_etl/exercise.py
python chapters/27_advanced_data_quality_rules/demo.py
python chapters/27_advanced_data_quality_rules/exercise.py
python chapters/28_schema_evolution/demo.py
python chapters/28_schema_evolution/exercise.py
python chapters/29_upsert_scd_concept/demo.py
python chapters/29_upsert_scd_concept/exercise.py
python chapters/30_mini_capstone_etl_project/demo.py
python chapters/30_mini_capstone_etl_project/exercise.py
python chapters/31_cli_arguments_etl_job/demo.py --input-path data/input/customer.csv --output-path data/output/chapter31/customers --run-date 2026-07-20 --write-mode overwrite
python chapters/31_cli_arguments_etl_job/exercise.py --input-path data/input/customer.csv --output-path data/output/chapter31/exercise --run-date 2026-07-20 --write-mode overwrite
python chapters/32_environment_config_dev_test_prod/demo.py --env dev
python chapters/32_environment_config_dev_test_prod/exercise.py --env dev
python chapters/33_data_contract_schema_validation/demo.py
python chapters/33_data_contract_schema_validation/exercise.py
python chapters/34_etl_monitoring_metrics/demo.py
python chapters/34_etl_monitoring_metrics/exercise.py
python chapters/35_retry_idempotent_etl/demo.py
python chapters/35_retry_idempotent_etl/exercise.py
python chapters/36_packaging_pyspark_project/demo.py
python chapters/36_packaging_pyspark_project/exercise.py
python final_project/etl_job.py
```

Chapter 12-36 la cac chapter nang cao. Mot so noi dung chi la demo hoc tap:

- Chapter 12 khong ket noi database that neu chua co JDBC driver, database, user va password.
- Chapter 17 la streaming demo don gian; exercise dung rate source va tu dung sau khoang 10 giay.
- Chapter 20 mo phong database ETL, khong bat buoc ket noi DB that.
- Chapter 22 la concept Kafka, chua can Kafka server that.
- Chapter 23 dung rate source va checkpoint demo cho streaming local.
- Chapter 24 chay bang pytest de test logic ETL nho.
- Chapter 26 doc config JSON va chi in write plan trong demo.
- Chapter 29 mo phong upsert/SCD bang DataFrame, khong can database that.
- Chapter 30 la mini capstone va chi show preview trong demo.
- Chapter 31 nhan tham so bang CLI va chi in write plan trong demo.
- Chapter 32 doc config theo dev/test/prod va chi in write plan trong demo.
- Chapter 33 co tinh demo case schema sai de thay error message ro rang.
- Chapter 34 tao metrics va ghi file metrics nho neu chay demo/exercise.
- Chapter 35 demo retry va idempotent write plan theo run_date.
- Chapter 36 demo cach tach transform thanh package/module rieng.
- Khong bat buoc chay het cac chapter nang cao neu moi can hoc concept hoac doc code mau.

## Git workflow

Tao branch rieng cho khoa hoc:

```bash
git checkout -b feature/pyspark-etl-course
```

Sau moi chapter, chi add cac file lien quan va commit ro noi dung:

```bash
git add chapters/02_basic_data_types_and_schemas
git commit -m "complete chapter 02 basic data types and schemas"

git add chapters/03_complex_data_types
git commit -m "complete chapter 03 complex data types"
```

Tiep tuc theo mau `complete chapter NN <topic>`. Voi final project:

```bash
git add final_project data/input shared README.md
git commit -m "complete customer etl final project"
```

Khong commit `.venv/`, `__pycache__/` hoac `data/output/`.

## Final project

```bash
cd /mnt/e/PySpark/pyspark-etl-course
source .venv-linux/bin/activate
python final_project/etl_job.py
```

Job doc customer/province, clean va validate du lieu, join mapping, tach valid/invalid, aggregate report, ghi output va summary.
