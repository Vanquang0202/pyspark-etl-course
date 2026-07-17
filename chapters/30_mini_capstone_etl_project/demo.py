import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql import Window
from pyspark.sql.functions import col, count, expr, initcap, trim, when

from shared.path_utils import DATA_INPUT
from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-30-mini-capstone-etl-project-demo")

    try:
        customer_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(DATA_INPUT / "chapter30_customer_raw.csv"))
        )

        province_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(DATA_INPUT / "province.csv"))
            .withColumn("province_code", trim(col("province_code")))
        )

        duplicate_window = Window.partitionBy("customer_id")

        cleaned_df = (
            customer_df
            .withColumn("customer_id", trim(col("customer_id")))
            .withColumn("customer_name", initcap(trim(col("customer_name"))))
            .withColumn("province_code", trim(col("province_code")))
            .withColumn("amount_number", expr("try_cast(amount as double)"))
            .withColumn("birth_date_parsed", expr("try_to_timestamp(birth_date, 'dd/MM/yyyy')").cast("date"))
        )

        joined_df = cleaned_df.join(province_df, on="province_code", how="left")

        quality_df = (
            joined_df
            .withColumn("customer_id_count", count("*").over(duplicate_window))
            .withColumn(
                "quality_status",
                when(col("customer_id").isNull() | (col("customer_id") == ""), "MISSING_CUSTOMER_ID")
                .when(col("customer_name").isNull() | (col("customer_name") == ""), "MISSING_NAME")
                .when(col("province_code").isNull() | (col("province_code") == ""), "MISSING_PROVINCE_CODE")
                .when(col("amount_number").isNull() | (col("amount_number") <= 0), "INVALID_AMOUNT")
                .when(col("birth_date_parsed").isNull(), "INVALID_BIRTH_DATE")
                .when(col("province_name").isNull(), "UNMAPPED_PROVINCE")
                .when(col("customer_id_count") > 1, "DUPLICATE_CUSTOMER_ID")
                .otherwise("OK"),
            )
        )

        valid_df = quality_df.filter(col("quality_status") == "OK")
        invalid_df = quality_df.filter(col("quality_status") != "OK")

        summary_df = (
            quality_df
            .groupBy("quality_status")
            .count()
            .orderBy("quality_status")
        )

        print("=== VALID CUSTOMERS ===")
        valid_df.select("customer_id", "customer_name", "province_name", "amount_number").show(truncate=False)

        print("=== INVALID CUSTOMERS ===")
        invalid_df.select(
            "customer_id",
            "customer_name",
            "province_code",
            "amount",
            "birth_date",
            "quality_status",
        ).show(truncate=False)

        print("=== SUMMARY REPORT ===")
        summary_df.show(truncate=False)

        print("Demo chi show preview, khong ghi output.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
