# PySpark ETL Course - Spark 4.0.1

Project chia bai hoc PySpark thanh tung chapter de de doc, chay, lam exercise va commit Git. Chapter 01 la DataFrame basics da hoan thanh. Chapter 02-06 bam theo PySpark Tour of Types va Python Package Management; Chapter 07-11 la cac ky nang ETL thuc hanh; Chapter 12-23 la cac chu de nang cao de mo rong ETL thuc te.

## Cau truc project

```text
pyspark-etl-course/
|-- data/
|   |-- input/
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
|   `-- 23_streaming_etl_with_checkpoint/
|-- shared/
|   |-- spark_utils.py
|   `-- path_utils.py
|-- final_project/
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
python final_project/etl_job.py
```

Chapter 12-23 la cac chapter nang cao. Mot so noi dung chi la demo hoc tap:

- Chapter 12 khong ket noi database that neu chua co JDBC driver, database, user va password.
- Chapter 17 la streaming demo don gian; exercise dung rate source va tu dung sau khoang 10 giay.
- Chapter 20 mo phong database ETL, khong bat buoc ket noi DB that.
- Chapter 22 la concept Kafka, chua can Kafka server that.
- Chapter 23 dung rate source va checkpoint demo cho streaming local.
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
