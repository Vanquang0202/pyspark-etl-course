import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from shared.path_utils import PROJECT_ROOT
from shared.spark_utils import create_spark_session


spark = create_spark_session("chapter-06-python-package-management-exercise")

assert os.environ["PYSPARK_PYTHON"] == sys.executable
assert os.environ["PYSPARK_DRIVER_PYTHON"] == sys.executable

shared_file = PROJECT_ROOT / "shared" / "path_utils.py"
spark.sparkContext.addPyFile(str(shared_file))

print("Driver and worker Python point to the active virtual environment:")
print(sys.executable)
print(f"Distributed Python file: {shared_file}")
print("This chapter packages code; it does not transform business data.")

check_results = [
    ("active_python_environment", "OK", sys.executable),
    ("distributed_python_file", "OK", str(shared_file)),
    (
        "business_data_transform",
        "SKIPPED",
        "This chapter packages code; it does not transform business data.",
    ),
]

check_df = spark.createDataFrame(
    check_results,
    ["check_name", "status", "detail"],
)

print("=== PACKAGE MANAGEMENT CHECKS ===")
check_df.show(truncate=False)

spark.stop()
