# Chapter 06 - Python Package Management

## Muc tieu

Day la chuong ve package va distribute Python code, khong phai chuong ETL transform.

Theo tai lieu Spark 4.0.1, khi job chay tren cluster, custom code va dependency phai co tren cac executor. PySpark native features co the gui file `.py`, package `.zip` hoac `.egg` bang:

- Config `spark.submit.pyFiles`.
- Tham so `spark-submit --py-files`.
- `SparkContext.addPyFile()` sau khi SparkContext da khoi dong.

Native features khong gui duoc Wheel va khong phu hop voi dependency co native code. Khi do can can nhac Conda, `venv-pack`, PEX hoac container image tuy cluster.

## Project hien tai

- `shared/spark_utils.py`: tao SparkSession, chon Python driver/worker va cau hinh local Spark.
- `shared/path_utils.py`: tinh project root va duong dan data dung chung.
- `requirements.txt`: khoa cac Python dependencies cua project.
- `.venv/`: moi truong Python 3.11 rieng cua project, khong commit Git.
- `PYSPARK_PYTHON`: Python executable cho worker/executor.
- `PYSPARK_DRIVER_PYTHON`: Python executable cho driver trong cach chay local hien tai.

`.venv` local khong tu dong duoc gui len cluster. Tren YARN/Kubernetes cluster mode, tai lieu canh bao khong set `PYSPARK_DRIVER_PYTHON` giong cach chay local.

## Vi du submit

```powershell
# Gui mot Python file/package zip khi submit job
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

Demo dung `addPyFile()` voi mot file `.py` local. Day chi la minh hoa API; loi ich quan trong nhat xuat hien khi driver va executor nam tren cac may khac nhau.

## Chay

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
