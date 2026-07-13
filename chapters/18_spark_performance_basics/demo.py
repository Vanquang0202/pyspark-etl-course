import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col

from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-18-spark-performance-basics-demo")

    try:
        source_df = spark.range(0, 20).withColumnRenamed("id", "customer_id")

        transformed_df = (
            source_df
            .withColumn("amount", col("customer_id") * 10)
            .filter(col("amount") >= 50)
            .select("customer_id", "amount")
        )

        print("=== TRANSFORMATION DA DUOC KHAI BAO, NHUNG CHUA CO ACTION ===")
        print("Spark moi chi tao logical plan.")

        print("=== EXPLAIN PLAN ===")
        transformed_df.explain()

        print("=== ACTION: SHOW ===")
        transformed_df.show(truncate=False)

        print("=== ACTION: COUNT ===")
        print(f"row_count: {transformed_df.count()}")

        print("=== SHUFFLE EXAMPLE: GROUP BY ===")
        grouped_df = transformed_df.groupBy((col("customer_id") % 3).alias("group_id")).count()
        grouped_df.explain()
        grouped_df.show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
