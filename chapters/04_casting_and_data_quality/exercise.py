import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, to_date, when
from pyspark.sql.types import DecimalType, StringType, StructField, StructType

from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-04-casting-and-data-quality-exercise")
    previous_ansi_setting = spark.conf.get("spark.sql.ansi.enabled")

    try:
        # Keep dirty source fields as strings, like raw CSV/API data.
        source_schema = StructType([
            StructField("customer_id", StringType(), nullable=True),
            StructField("name", StringType(), nullable=True),
            StructField("birth_date", StringType(), nullable=True),
            StructField("province_code", StringType(), nullable=True),
            StructField("amount", StringType(), nullable=True),
        ])

        source_data = [
            ("001", "Nguyen Van A", "01/01/1995", "01", "100000.50"),
            ("002", "Tran Thi B", "15/05/1998", "79", "abc"),
            ("003", "Le Van C", "20/09/2000", "48", "-100"),
            ("004", "Pham Thi D", "1999-10-10", "01", "120000"),
            (None, "Missing ID", "11/11/2001", "99", "150000"),
            ("006", "Missing Province", "05/12/1997", None, "70000"),
        ]

        source_df = spark.createDataFrame(source_data, schema=source_schema)

        print("=== RAW STRING DATA ===")
        source_df.show(truncate=False)
        source_df.printSchema()

        # Invalid casts become null so they can be classified instead of stopping the job.
        spark.conf.set("spark.sql.ansi.enabled", False)

        checked_df = (
            source_df
            .withColumn("customer_id_int", col("customer_id").cast("integer"))
            .withColumn("amount_decimal", col("amount").cast(DecimalType(18, 2)))
            .withColumn("birth_date_parsed", to_date(col("birth_date"), "dd/MM/yyyy"))
            .withColumn(
                "data_quality_status",
                when(
                    col("customer_id").isNull() | col("province_code").isNull(),
                    "MISSING_REQUIRED_FIELD",
                )
                .when(
                    col("amount_decimal").isNull() | (col("amount_decimal") <= 0),
                    "INVALID_AMOUNT",
                )
                .when(col("birth_date_parsed").isNull(), "INVALID_BIRTH_DATE")
                .otherwise("OK"),
            )
        )

        valid_df = checked_df.filter(col("data_quality_status") == "OK")
        invalid_df = checked_df.filter(col("data_quality_status") != "OK")

        total_records = checked_df.count()
        valid_records = valid_df.count()
        invalid_records = invalid_df.count()

        print("=== DATA QUALITY COUNTS ===")
        print(f"total_records: {total_records}")
        print(f"valid_records: {valid_records}")
        print(f"invalid_records: {invalid_records}")

        print("=== VALID DATA ===")
        valid_df.show(truncate=False)

        print("=== INVALID DATA ===")
        invalid_df.show(truncate=False)
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi_setting)
        spark.stop()


if __name__ == "__main__":
    main()
