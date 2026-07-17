import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql import Window
from pyspark.sql.functions import col, count, expr, trim, when

from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-27-advanced-data-quality-rules-exercise")

    try:
        order_df = spark.createDataFrame(
            [
                ("O001", "C001", "01", "250000", "17/07/2026"),
                ("O002", "C002", "79", "0", "17/07/2026"),
                ("O003", "C003", "XX", "50000", "17/07/2026"),
                ("O003", "C004", "48", "60000", "17/07/2026"),
                ("O005", "", "01", "70000", "2026-07-17"),
            ],
            ["order_id", "customer_id", "province_code", "amount", "order_date"],
        )

        valid_province_codes = ["01", "48", "79"]
        duplicate_window = Window.partitionBy("order_id")

        checked_df = (
            order_df
            .withColumn("order_id", trim(col("order_id")))
            .withColumn("customer_id", trim(col("customer_id")))
            .withColumn("province_code", trim(col("province_code")))
            .withColumn("amount_number", expr("try_cast(amount as double)"))
            .withColumn("order_date_parsed", expr("try_to_timestamp(order_date, 'dd/MM/yyyy')").cast("date"))
            .withColumn("order_id_count", count("*").over(duplicate_window))
            .withColumn(
                "quality_status",
                when(col("order_id").isNull() | (col("order_id") == ""), "MISSING_ORDER_ID")
                .when(col("customer_id").isNull() | (col("customer_id") == ""), "MISSING_CUSTOMER_ID")
                .when(col("amount_number").isNull() | (col("amount_number") <= 0), "INVALID_AMOUNT")
                .when(~col("province_code").isin(valid_province_codes), "INVALID_PROVINCE_CODE")
                .when(col("order_date_parsed").isNull(), "INVALID_DATE_FORMAT")
                .when(col("order_id_count") > 1, "DUPLICATE_ORDER_ID")
                .otherwise("OK"),
            )
        )

        print("=== VALID ORDERS ===")
        checked_df.filter(col("quality_status") == "OK").show(truncate=False)

        print("=== INVALID ORDERS ===")
        checked_df.filter(col("quality_status") != "OK").show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
