import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, trim, when

from shared.path_utils import DATA_INPUT
from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-16-cache-persist-exercise")

    try:
        customer_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(DATA_INPUT / "customer.csv"))
        )

        quality_df = (
            customer_df
            .withColumn("customer_name", trim(col("name")))
            .withColumn("amount_number", col("amount").cast("double"))
            .withColumn(
                "is_valid",
                col("customer_id").isNotNull()
                & col("customer_name").isNotNull()
                & (col("customer_name") != "")
                & col("amount_number").isNotNull()
                & (col("amount_number") > 0),
            )
            .withColumn("quality_status", when(col("is_valid"), "VALID").otherwise("INVALID"))
            .cache()
        )

        print(f"total_rows: {quality_df.count()}")
        print(f"valid_rows: {quality_df.filter(col('quality_status') == 'VALID').count()}")
        print(f"invalid_rows: {quality_df.filter(col('quality_status') == 'INVALID').count()}")

        print("=== QUALITY SUMMARY ===")
        quality_df.groupBy("quality_status").count().show(truncate=False)

        quality_df.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
