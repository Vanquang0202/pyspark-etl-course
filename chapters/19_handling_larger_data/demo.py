import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, sum as spark_sum

from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-19-handling-larger-data-demo")

    try:
        raw_df = (
            spark.range(0, 100_000)
            .withColumnRenamed("id", "transaction_id")
            .withColumn("customer_id", (col("transaction_id") % 1_000) + 1)
            .withColumn("amount", (col("transaction_id") % 500) + 10)
            .withColumn("unused_big_column", col("transaction_id") * 999)
        )

        print(f"initial_partitions: {raw_df.rdd.getNumPartitions()}")

        small_df = (
            raw_df
            .filter(col("amount") >= 300)
            .select("transaction_id", "customer_id", "amount")
            .repartition(4, "customer_id")
        )

        print(f"after_repartition: {small_df.rdd.getNumPartitions()}")

        summary_df = (
            small_df
            .groupBy("customer_id")
            .agg(spark_sum("amount").alias("total_amount"))
            .orderBy(col("total_amount").desc())
        )

        print("=== TOP CUSTOMER SUMMARY ===")
        summary_df.show(10, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
