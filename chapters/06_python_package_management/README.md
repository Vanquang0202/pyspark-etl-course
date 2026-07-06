# Chapter 06 - Python Package Management

## Mục tiêu

Đây là chương về package và distribute Python code, không phải chương ETL transform.

Theo tài liệu Spark 4.0.1, khi job chạy trên cluster, custom code và dependency phải có trên các executor. PySpark native features có thể gửi file `.py`, package `.zip` hoặc `.egg` bằng:

- Config `spark.submit.pyFiles`.
- Tham số `spark-submit --py-files`.
- `SparkContext.addPyFile()` sau khi SparkContext đã khởi động.

Native features không gửi được Wheel và không phù hợp với dependency có native code. Khi đó cần cân nhắc Conda, `venv-pack`, PEX hoặc container image tùy cluster.

## Project hiện tại

- `shared/spark_utils.py`: tạo SparkSession, chọn Python driver/worker và cấu hình local Spark.
- `shared/path_utils.py`: tính project root và đường dẫn data dùng chung.
- `requirements.txt`: khóa các Python dependencies của project.
- `.venv/`: môi trường Python 3.11 riêng của project, không commit Git.
- `PYSPARK_PYTHON`: Python executable cho worker/executor.
- `PYSPARK_DRIVER_PYTHON`: Python executable cho driver trong cách chạy local hiện tại.

`.venv` local không tự động được gửi lên cluster. Trên YARN/Kubernetes cluster mode, tài liệu cảnh báo không set `PYSPARK_DRIVER_PYTHON` giống cách chạy local.

## Ví dụ submit

```powershell
# Gửi một Python file/package zip khi submit job
spark-submit --py-files shared_code.zip app.py
```

```python
spark = (
    SparkSession.builder
    .config("spark.submit.pyFiles", "shared_code.zip")
    .getOrCreate()
)

spark.sparkContext.addPyFile("shared_code.zip")
```

Demo dùng `addPyFile()` với một file `.py` local. Đây chỉ là minh họa API; lợi ích quan trọng nhất xuất hiện khi driver và executor nằm trên các máy khác nhau.

## Chạy

```powershell
python chapters/06_python_package_management/demo.py
python chapters/06_python_package_management/exercise.py
```

## Quick Notes

### Bài này học gì?

* Hiểu cách chia sẻ Python code khi chạy PySpark.
* Biết driver và executor đều cần thấy custom code/dependency.
* Phân biệt môi trường local với cách distribute code trên cluster.

### Khái niệm chính

* `spark.submit.pyFiles`: cấu hình gửi file `.py`, `.zip` hoặc `.egg`.
* `--py-files`: tham số `spark-submit` để gửi code Python.
* `addPyFile()`: thêm file Python sau khi SparkContext đã chạy.
* Driver: process điều phối Spark job.
* Executor: process thực thi task trên dữ liệu.
* `PYSPARK_PYTHON`: Python executable dùng cho worker/executor.

### Nhớ nhanh

* `.venv` local không tự động có trên cluster.
* Shared code nên được package hoặc submit rõ ràng khi chạy phân tán.
