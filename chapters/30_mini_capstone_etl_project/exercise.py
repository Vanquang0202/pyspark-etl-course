import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql import Window
from pyspark.sql.functions import col, count, expr, sum as spark_sum, trim, when

from shared.path_utils import DATA_INPUT
from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-30-mini-capstone-etl-project-exercise")

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

        prepared_df = (
            customer_df
            .withColumn("customer_id", trim(col("customer_id")))
            .withColumn("customer_name", trim(col("customer_name")))
            .withColumn("province_code", trim(col("province_code")))
            .withColumn("amount_number", expr("try_cast(amount as double)"))
            .withColumn("birth_date_parsed", expr("try_to_timestamp(birth_date, 'dd/MM/yyyy')").cast("date"))
            .join(province_df, on="province_code", how="left")
            .withColumn("customer_id_count", count("*").over(duplicate_window))
            .withColumn(
                "quality_status",
                when(col("customer_id").isNull() | (col("customer_id") == ""), "MISSING_CUSTOMER_ID")
                .when(col("customer_name").isNull() | (col("customer_name") == ""), "MISSING_NAME")
                .when(col("amount_number").isNull() | (col("amount_number") <= 0), "INVALID_AMOUNT")
                .when(col("birth_date_parsed").isNull(), "INVALID_BIRTH_DATE")
                .when(col("province_name").isNull(), "UNMAPPED_PROVINCE")
                .when(col("customer_id_count") > 1, "DUPLICATE_CUSTOMER_ID")
                .otherwise("OK"),
            )
        )

        valid_df = prepared_df.filter(col("quality_status") == "OK")
        invalid_df = prepared_df.filter(col("quality_status") != "OK")

        province_report_df = (
            valid_df
            .groupBy("province_name")
            .agg(
                count("*").alias("customer_count"),
                spark_sum("amount_number").alias("total_amount"),
            )
            .orderBy("province_name")
        )

        print("=== VALID COUNT ===")
        print(valid_df.count())

        print("=== INVALID COUNT ===")
        print(invalid_df.count())

        print("=== PROVINCE REPORT FROM VALID RECORDS ===")
        province_report_df.show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
