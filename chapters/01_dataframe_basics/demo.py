import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))

from shared.spark_utils import create_spark_session

spark = create_spark_session("chapter-01-dataframe-basics")

data = [
    ("001", "Nguyen Van A", 25, "HN"),
    ("002", "Tran Thi B", 30, "HCM"),
    ("003", "Le Van C", 28, "DN"),
]
columns = ["id", "name", "age", "province"]

df = spark.createDataFrame(data, columns)

print("=== DATA ===")
df.show()

print("=== SCHEMA ===")
df.printSchema()

print("=== COUNT ===")
print(df.count())

spark.stop()
