import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, trim, upper, year, to_date, when

from shared.path_utils import DATA_INPUT
from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-08-data-manipulation-demo")
    previous_ansi_setting = spark.conf.get("spark.sql.ansi.enabled")

    try:
        # Invalid dates become null so learners can inspect data quality issues.
        spark.conf.set("spark.sql.ansi.enabled", False)

        customer_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(DATA_INPUT / "customer.csv"))
        )

        print("=== ORIGINAL SCHEMA ===")
        customer_df.printSchema()

        selected_df = customer_df.select(
            "customer_id",
            "name",
            "gender",
            "birth_date",
            "province_code",
            "amount",
        )

        result_df = (
            selected_df
            .withColumnRenamed("name", "customer_name")
            .withColumn("customer_name", trim(col("customer_name")))
            .withColumn("province_code_clean", upper(trim(col("province_code"))))
            .withColumn("amount", col("amount").cast("double"))
            .filter(col("amount") > 0)
            .withColumn(
                "amount_level",
                when(col("amount") >= 1000000, "HIGH")
                .when(col("amount") >= 500000, "MEDIUM")
                .otherwise("LOW"),
            )
            .withColumn("birth_date", to_date(col("birth_date"), "dd/MM/yyyy"))
            .withColumn("birth_year", year(col("birth_date")))
            .drop("province_code")
            .distinct()
            .orderBy(col("amount").desc())
        )

        print("=== FINAL DATA AFTER MANIPULATION ===")
        result_df.show(truncate=False)

        print("=== FINAL SCHEMA ===")
        result_df.printSchema()
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi_setting)
        spark.stop()


if __name__ == "__main__":
    main()
