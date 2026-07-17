import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, lit

from shared.spark_utils import create_spark_session


def align_order_schema(order_df):
    aligned_df = order_df

    if "discount_amount" not in aligned_df.columns:
        aligned_df = aligned_df.withColumn("discount_amount", lit(0).cast("double"))

    if "order_status" not in aligned_df.columns:
        aligned_df = aligned_df.withColumn("order_status", lit("NEW"))

    return (
        aligned_df
        .withColumn("order_id", col("order_id").cast("string"))
        .withColumn("customer_id", col("customer_id").cast("string"))
        .withColumn("amount", col("amount").cast("double"))
        .withColumn("discount_amount", col("discount_amount").cast("double"))
        .withColumn("order_status", col("order_status").cast("string"))
        .select("order_id", "customer_id", "amount", "discount_amount", "order_status")
    )


def main() -> None:
    spark = create_spark_session("chapter-28-schema-evolution-exercise")

    try:
        source_v1_df = spark.createDataFrame(
            [("O001", "C001", "100000")],
            ["order_id", "customer_id", "amount"],
        )

        source_v2_df = spark.createDataFrame(
            [("O002", "C002", 200000.0, 15000.0, "PAID")],
            ["order_id", "customer_id", "amount", "discount_amount", "order_status"],
        )

        print("=== ORDER V1 ALIGNED ===")
        align_order_schema(source_v1_df).show(truncate=False)

        print("=== ORDER V2 ALIGNED ===")
        align_order_schema(source_v2_df).show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
