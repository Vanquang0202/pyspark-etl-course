import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql import Window
from pyspark.sql.functions import col, count, expr, trim, when

from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-27-advanced-data-quality-rules-demo")

    try:
        source_df = spark.createDataFrame(
            [
                ("C001", "Nguyen Van An", "01", "100000", "01/01/1995"),
                ("C002", "Tran Thi Binh", "79", "-50", "15/05/1998"),
                ("C003", "Le Van Cuong", "99", "200000", "20/09/2000"),
                ("C003", "Le Van Cuong 2", "01", "300000", "21/09/2000"),
                ("", "Pham Van Dung", "48", "150000", "10/10/1999"),
                ("C005", "Hoang Thi Em", "79", "120000", "31/02/1996"),
            ],
            ["customer_id", "customer_name", "province_code", "amount", "birth_date"],
        )

        valid_province_codes = ["01", "48", "79"]
        duplicate_window = Window.partitionBy("customer_id")

        checked_df = (
            source_df
            .withColumn("customer_id", trim(col("customer_id")))
            .withColumn("province_code", trim(col("province_code")))
            .withColumn("amount_number", expr("try_cast(amount as double)"))
            .withColumn("birth_date_parsed", expr("try_to_timestamp(birth_date, 'dd/MM/yyyy')").cast("date"))
            .withColumn("customer_id_count", count("*").over(duplicate_window))
            .withColumn(
                "quality_status",
                when(col("customer_id").isNull() | (col("customer_id") == ""), "MISSING_CUSTOMER_ID")
                .when(col("customer_name").isNull() | (trim(col("customer_name")) == ""), "MISSING_NAME")
                .when(col("province_code").isNull() | (col("province_code") == ""), "MISSING_PROVINCE_CODE")
                .when(col("amount_number").isNull() | (col("amount_number") <= 0), "INVALID_AMOUNT")
                .when(~col("province_code").isin(valid_province_codes), "INVALID_PROVINCE_CODE")
                .when(col("birth_date_parsed").isNull(), "INVALID_DATE_FORMAT")
                .when(col("customer_id_count") > 1, "DUPLICATE_CUSTOMER_ID")
                .otherwise("OK"),
            )
        )

        valid_df = checked_df.filter(col("quality_status") == "OK")
        invalid_df = checked_df.filter(col("quality_status") != "OK")

        print("=== VALID RECORDS ===")
        valid_df.show(truncate=False)

        print("=== INVALID RECORDS ===")
        invalid_df.select(
            "customer_id",
            "customer_name",
            "province_code",
            "amount",
            "birth_date",
            "quality_status",
        ).show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
