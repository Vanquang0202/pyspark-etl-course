import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, trim, to_date, when
from pyspark.sql.types import DecimalType

from shared.path_utils import DATA_INPUT
from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-04-casting-and-data-quality-demo")
    previous_ansi_setting = spark.conf.get("spark.sql.ansi.enabled")

    try:
        # ANSI is disabled here so invalid casts become null for data-quality checks.
        spark.conf.set("spark.sql.ansi.enabled", False)

        customer_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(DATA_INPUT / "customer.csv"))
        )

        print("=== SOURCE SCHEMA: CSV FIELDS ARE READ AS STRING ===")
        customer_df.printSchema()

        casted_df = (
            customer_df
            .withColumn("name", trim(col("name")))
            .withColumn("customer_id_int", col("customer_id").cast("integer"))
            # Province code is a category, not a quantity. Keep "01" as a string.
            .withColumn("province_code_str", trim(col("province_code").cast("string")))
            .withColumn("amount_decimal", col("amount").cast(DecimalType(18, 2)))
            .withColumn("birth_date_parsed", to_date(col("birth_date"), "dd/MM/yyyy"))
            .withColumn(
                "is_valid_customer_id",
                col("customer_id").isNotNull()
                & (trim(col("customer_id")) != "")
                & col("customer_id_int").isNotNull(),
            )
            .withColumn(
                "is_valid_amount",
                col("amount_decimal").isNotNull() & (col("amount_decimal") > 0),
            )
            .withColumn(
                "is_valid_birth_date",
                col("birth_date_parsed").isNotNull(),
            )
            .withColumn(
                "data_quality_status",
                when(
                    col("customer_id").isNull()
                    | (trim(col("customer_id")) == "")
                    | col("province_code").isNull()
                    | (trim(col("province_code")) == ""),
                    "MISSING_REQUIRED_FIELD",
                )
                .when(~col("is_valid_amount"), "INVALID_AMOUNT")
                .when(~col("is_valid_birth_date"), "INVALID_BIRTH_DATE")
                .when(~col("is_valid_customer_id"), "MISSING_REQUIRED_FIELD")
                .otherwise("OK"),
            )
        )

        print("=== CASTED DATA WITH QUALITY FLAGS ===")
        casted_df.select(
            "customer_id",
            "customer_id_int",
            "name",
            "province_code_str",
            "amount",
            "amount_decimal",
            "birth_date",
            "birth_date_parsed",
            "is_valid_customer_id",
            "is_valid_amount",
            "is_valid_birth_date",
            "data_quality_status",
        ).show(truncate=False)

        print("=== SCHEMA AFTER CASTING ===")
        casted_df.printSchema()
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi_setting)
        spark.stop()


if __name__ == "__main__":
    main()
