import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, lit

from shared.spark_utils import create_spark_session


TARGET_COLUMNS = ["customer_id", "customer_name", "province_code", "amount", "email"]


def align_customer_schema(customer_df):
    aligned_df = customer_df

    if "email" not in aligned_df.columns:
        aligned_df = aligned_df.withColumn("email", lit(None).cast("string"))

    if "province_code" not in aligned_df.columns:
        aligned_df = aligned_df.withColumn("province_code", lit("UNKNOWN"))

    return (
        aligned_df
        .withColumn("customer_id", col("customer_id").cast("string"))
        .withColumn("customer_name", col("customer_name").cast("string"))
        .withColumn("province_code", col("province_code").cast("string"))
        .withColumn("amount", col("amount").cast("double"))
        .withColumn("email", col("email").cast("string"))
        .select(*TARGET_COLUMNS)
    )


def main() -> None:
    spark = create_spark_session("chapter-28-schema-evolution-demo")

    try:
        old_source_df = spark.createDataFrame(
            [("C001", "Nguyen Van An", "01", "100000")],
            ["customer_id", "customer_name", "province_code", "amount"],
        )

        new_source_df = spark.createDataFrame(
            [("C002", "Tran Thi Binh", "79", 250000.0, "binh@example.com")],
            ["customer_id", "customer_name", "province_code", "amount", "email"],
        )

        missing_column_df = spark.createDataFrame(
            [("C003", "Le Van Cuong", "300000")],
            ["customer_id", "customer_name", "amount"],
        )

        print("=== OLD SOURCE ALIGNED ===")
        align_customer_schema(old_source_df).show(truncate=False)

        print("=== NEW SOURCE WITH EXTRA COLUMN ALIGNED ===")
        align_customer_schema(new_source_df).show(truncate=False)

        print("=== SOURCE MISSING PROVINCE_CODE ALIGNED ===")
        align_customer_schema(missing_column_df).show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
