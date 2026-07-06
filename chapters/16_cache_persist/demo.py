import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, trim, when

from shared.path_utils import DATA_INPUT
from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-16-cache-persist-demo")

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
                "data_quality_status",
                when(col("customer_id").isNull(), "MISSING_CUSTOMER_ID")
                .when(col("customer_name").isNull() | (col("customer_name") == ""), "MISSING_NAME")
                .when(col("amount_number").isNull() | (col("amount_number") <= 0), "INVALID_AMOUNT")
                .otherwise("OK"),
            )
        )

        print("=== ACTIONS BEFORE CACHE ===")
        print(f"total_rows: {quality_df.count()}")
        print(f"valid_rows: {quality_df.filter(col('data_quality_status') == 'OK').count()}")

        cached_quality_df = quality_df.cache()

        print("=== MATERIALIZE CACHE ===")
        print(f"cached_rows: {cached_quality_df.count()}")

        print("=== ACTIONS USING CACHED DATAFRAME ===")
        cached_quality_df.groupBy("data_quality_status").count().show(truncate=False)
        cached_quality_df.select("customer_id", "customer_name", "data_quality_status").show(truncate=False)

        cached_quality_df.unpersist()
        print("cache_released: true")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
