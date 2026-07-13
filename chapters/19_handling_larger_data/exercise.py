import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, count, sum as spark_sum

from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-19-handling-larger-data-exercise")

    try:
        orders_df = (
            spark.range(0, 200_000)
            .withColumnRenamed("id", "order_id")
            .withColumn("customer_id", (col("order_id") % 5_000) + 1)
            .withColumn("province_code", (col("order_id") % 10).cast("string"))
            .withColumn("amount", (col("order_id") % 1_000) + 1)
            .withColumn("debug_text", col("order_id").cast("string"))
        )

        province_df = spark.createDataFrame(
            [
                ("0", "North"),
                ("1", "South"),
                ("2", "East"),
                ("3", "West"),
            ],
            ["province_code", "province_group"],
        )

        prepared_df = (
            orders_df
            .filter(col("amount") >= 500)
            .select("order_id", "customer_id", "province_code", "amount")
            .repartition(6, "province_code")
        )

        joined_df = prepared_df.join(province_df, on="province_code", how="inner")

        report_df = (
            joined_df
            .groupBy("province_group")
            .agg(
                count("*").alias("order_count"),
                spark_sum("amount").alias("total_amount"),
            )
            .orderBy("province_group")
        )

        print("=== LARGE DATA REPORT SAMPLE ===")
        report_df.show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
