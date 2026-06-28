import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyspark.sql.functions import col, trim, to_date, year, count, sum as spark_sum
from shared.spark_utils import create_spark_session
from shared.path_utils import DATA_INPUT, DATA_OUTPUT


def main() -> None:
    spark = create_spark_session("final-project-customer-etl")

    customer_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(str(DATA_INPUT / "customer.csv"))
    )

    province_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(str(DATA_INPUT / "province.csv"))
    )

    customer_clean_df = (
        customer_df
        .filter(col("customer_id").isNotNull())
        .withColumn("name", trim(col("name")))
        .withColumn("birth_date", to_date(col("birth_date"), "dd/MM/yyyy"))
        .withColumn("birth_year", year(col("birth_date")))
        .withColumn("amount", col("amount").cast("long"))
    )

    customer_enriched_df = (
        customer_clean_df
        .join(province_df, on="province_code", how="left")
    )

    invalid_province_df = (
        customer_clean_df
        .join(province_df, on="province_code", how="left_anti")
    )

    report_df = (
        customer_enriched_df
        .groupBy("province_name")
        .agg(
            count("*").alias("total_customer"),
            spark_sum("amount").alias("total_amount")
        )
        .orderBy("province_name")
    )

    print("=== CUSTOMER CLEAN ===")
    customer_enriched_df.show(truncate=False)

    print("=== INVALID PROVINCE ===")
    invalid_province_df.show(truncate=False)

    print("=== REPORT BY PROVINCE ===")
    report_df.show(truncate=False)

    base_output = DATA_OUTPUT / "final"

    customer_enriched_df.write.mode("overwrite").parquet(str(base_output / "customer_clean"))

    report_df.write.mode("overwrite").option("header", True).csv(str(base_output / "customer_by_province"))

    invalid_province_df.write.mode("overwrite").option("header", True).csv(str(base_output / "invalid_province"))

    spark.stop()


if __name__ == "__main__":
    main()
