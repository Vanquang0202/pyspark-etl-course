import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from shared.path_utils import PROJECT_ROOT
from shared.spark_utils import create_spark_session


spark = create_spark_session("chapter-06-python-package-management")

print("=== LOCAL PYTHON ENVIRONMENT ===")
print(f"sys.executable: {sys.executable}")
print(f"PYSPARK_PYTHON: {os.environ.get('PYSPARK_PYTHON')}")
print(f"PYSPARK_DRIVER_PYTHON: {os.environ.get('PYSPARK_DRIVER_PYTHON')}")
print(f"requirements.txt: {PROJECT_ROOT / 'requirements.txt'}")
print(f"shared package: {PROJECT_ROOT / 'shared'}")

python_file = PROJECT_ROOT / "shared" / "path_utils.py"
spark.sparkContext.addPyFile(str(python_file))

print("=== PYSPARK NATIVE DISTRIBUTION ===")
print(f"Added with SparkContext.addPyFile(): {python_file}")
print("Equivalent submit option: spark-submit --py-files <file-or-package.zip> app.py")
print("Equivalent config: spark.submit.pyFiles=<file-or-package.zip>")

spark.stop()
