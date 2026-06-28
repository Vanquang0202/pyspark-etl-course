import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))

from shared.spark_utils import create_spark_session
from shared.path_utils import DATA_INPUT, DATA_OUTPUT

spark = create_spark_session("chapter-07-read-write-files")

customer_path = str(DATA_INPUT / "customer.csv")
output_path = str(DATA_OUTPUT / "chapter07" / "customer_parquet")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(customer_path)
)

print("=== INPUT CUSTOMER ===")
df.show(truncate=False)
df.printSchema()

(
    df.write
    .mode("overwrite")
    .parquet(output_path)
)

print(f"Wrote parquet to: {output_path}")

spark.stop()
