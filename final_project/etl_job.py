import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_date,
    initcap,
    max,
    min,
    sum as spark_sum,
    to_date,
    trim,
    when,
)

from final_project.config import (
    CUSTOMER_RAW_PATH,
    ETL_SUMMARY_PATH,
    FALLBACK_CUSTOMER_PATH,
    INVALID_CUSTOMERS_PATH,
    PROVINCE_PATH,
    PROVINCE_SEGMENT_REPORT_PATH,
    VALID_CUSTOMERS_PATH,
)
from shared.spark_utils import create_spark_session


def read_csv(spark, path: Path) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(str(path))
    )


def build_customer_quality_df(customer_df: DataFrame, province_df: DataFrame) -> DataFrame:
    cleaned_customer_df = (
        customer_df
        .withColumnRenamed("customer_id", "customer_id_raw")
        .withColumnRenamed("name", "name_raw")
        .withColumnRenamed("birth_date", "birth_date_raw")
        .withColumnRenamed("province_code", "province_code_raw")
        .withColumnRenamed("amount", "amount_raw")
        # Customer ID is kept as string because IDs/codes can have leading zeroes.
        .withColumn("customer_id", trim(col("customer_id_raw")))
        .withColumn("customer_name", initcap(trim(col("name_raw"))))
        .withColumn("province_code", trim(col("province_code_raw")))
        .withColumn("amount", col("amount_raw").cast("double"))
        .withColumn("birth_date", to_date(col("birth_date_raw"), "dd/MM/yyyy"))
        .withColumn("processing_date", current_date())
    )

    province_clean_df = (
        province_df
        .withColumn("province_code", trim(col("province_code")))
        .select("province_code", "province_name")
    )

    joined_df = cleaned_customer_df.join(province_clean_df, on="province_code", how="left")

    return (
        joined_df
        .withColumn(
            "mapping_status",
            when(col("province_name").isNotNull(), "MAPPED").otherwise("UNMAPPED"),
        )
        .withColumn(
            "data_quality_status",
            when(col("customer_id").isNull() | (col("customer_id") == ""), "MISSING_CUSTOMER_ID")
            .when(col("customer_name").isNull() | (col("customer_name") == ""), "MISSING_NAME")
            .when(col("province_code").isNull() | (col("province_code") == ""), "MISSING_PROVINCE_CODE")
            .when(col("amount").isNull() | (col("amount") <= 0), "INVALID_AMOUNT")
            .when(col("birth_date").isNull(), "INVALID_BIRTH_DATE")
            .when(col("province_name").isNull(), "UNMAPPED_PROVINCE")
            .otherwise("OK"),
        )
    )


def build_valid_customers_df(quality_df: DataFrame) -> DataFrame:
    return (
        quality_df
        .filter(col("data_quality_status") == "OK")
        .withColumn(
            "customer_segment",
            when(col("amount") >= 1000000, "VIP")
            .when(col("amount") >= 500000, "STANDARD")
            .otherwise("BASIC"),
        )
        .select(
            "customer_id",
            "customer_name",
            "birth_date",
            "province_code",
            "province_name",
            "amount",
            "customer_segment",
            "processing_date",
        )
    )


def build_invalid_customers_df(quality_df: DataFrame) -> DataFrame:
    return (
        quality_df
        .filter(col("data_quality_status") != "OK")
        .select(
            "customer_id_raw",
            "name_raw",
            "birth_date_raw",
            "province_code_raw",
            "amount_raw",
            "mapping_status",
            "data_quality_status",
            "processing_date",
        )
    )


def build_report_df(valid_df: DataFrame) -> DataFrame:
    return (
        valid_df
        .groupBy("province_name", "customer_segment")
        .agg(
            count("*").alias("customer_count"),
            spark_sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount"),
            max("amount").alias("max_amount"),
            min("amount").alias("min_amount"),
        )
        .orderBy(col("province_name").asc(), col("total_amount").desc())
    )


def build_summary_df(
    spark,
    total_records: int,
    valid_records: int,
    invalid_records: int,
    unmapped_records: int,
    report_rows: int,
) -> DataFrame:
    return (
        spark.createDataFrame(
            [
                (
                    total_records,
                    valid_records,
                    invalid_records,
                    unmapped_records,
                    report_rows,
                )
            ],
            schema=[
                "total_records",
                "valid_records",
                "invalid_records",
                "unmapped_records",
                "report_rows",
            ],
        )
        .withColumn("processing_date", current_date())
    )


def main() -> None:
    spark = create_spark_session("final-project-etl-job")
    previous_ansi_setting = spark.conf.get("spark.sql.ansi.enabled")

    try:
        # Invalid casts and invalid dates become null, then data quality rules reject them.
        spark.conf.set("spark.sql.ansi.enabled", False)

        customer_path = CUSTOMER_RAW_PATH if CUSTOMER_RAW_PATH.is_file() else FALLBACK_CUSTOMER_PATH

        print("=== INPUT PATHS ===")
        print(f"customer_input: {customer_path}")
        print(f"province_input: {PROVINCE_PATH}")

        customer_df = read_csv(spark, customer_path)
        province_df = read_csv(spark, PROVINCE_PATH)

        print("=== CUSTOMER INPUT SCHEMA ===")
        customer_df.printSchema()

        print("=== PROVINCE INPUT SCHEMA ===")
        province_df.printSchema()

        quality_df = build_customer_quality_df(customer_df, province_df)
        valid_df = build_valid_customers_df(quality_df)
        invalid_df = build_invalid_customers_df(quality_df)
        report_df = build_report_df(valid_df)

        total_records = quality_df.count()
        valid_records = valid_df.count()
        invalid_records = invalid_df.count()
        unmapped_records = quality_df.filter(col("data_quality_status") == "UNMAPPED_PROVINCE").count()
        report_rows = report_df.count()

        summary_df = build_summary_df(
            spark,
            total_records,
            valid_records,
            invalid_records,
            unmapped_records,
            report_rows,
        )

        print("=== ETL SUMMARY ===")
        print(f"total_records: {total_records}")
        print(f"valid_records: {valid_records}")
        print(f"invalid_records: {invalid_records}")
        print(f"unmapped_records: {unmapped_records}")
        print(f"report_rows: {report_rows}")

        print("=== VALID CUSTOMERS SAMPLE ===")
        valid_df.show(truncate=False)

        print("=== INVALID CUSTOMERS ===")
        invalid_df.show(truncate=False)

        print("=== PROVINCE SEGMENT REPORT ===")
        report_df.show(truncate=False)

        valid_df.write.mode("overwrite").parquet(str(VALID_CUSTOMERS_PATH))
        invalid_df.write.mode("overwrite").option("header", True).csv(str(INVALID_CUSTOMERS_PATH))
        report_df.write.mode("overwrite").option("header", True).csv(str(PROVINCE_SEGMENT_REPORT_PATH))
        summary_df.write.mode("overwrite").option("header", True).csv(str(ETL_SUMMARY_PATH))

        print("=== OUTPUT PATHS ===")
        print(f"valid_customers_parquet: {VALID_CUSTOMERS_PATH}")
        print(f"invalid_customers_csv: {INVALID_CUSTOMERS_PATH}")
        print(f"province_segment_report_csv: {PROVINCE_SEGMENT_REPORT_PATH}")
        print(f"etl_summary_csv: {ETL_SUMMARY_PATH}")
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi_setting)
        spark.stop()


if __name__ == "__main__":
    main()
