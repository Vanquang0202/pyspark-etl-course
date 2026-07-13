import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, when

from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-18-spark-performance-basics-exercise")

    try:
        orders_df = spark.range(1, 31).withColumnRenamed("id", "order_id")

        prepared_df = (
            orders_df
            .withColumn("customer_id", (col("order_id") % 5) + 1)
            .withColumn("amount", col("order_id") * 7)
            .withColumn(
                "amount_group",
                when(col("amount") >= 100, "HIGH").otherwise("NORMAL"),
            )
            .filter(col("amount") > 30)
        )

        print("=== EXPLAIN BEFORE ACTION ===")
        prepared_df.explain()

        print("=== ACTION: LIMIT + SHOW ===")
        prepared_df.limit(10).show(truncate=False)

        print("=== ACTION: COUNT ===")
        print(f"total_orders: {prepared_df.count()}")

        print("=== GROUP BY CO THE TAO SHUFFLE ===")
        summary_df = prepared_df.groupBy("customer_id", "amount_group").count()
        summary_df.explain()
        summary_df.show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
