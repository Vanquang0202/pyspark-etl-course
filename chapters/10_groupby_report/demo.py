import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import avg, col, count, max, min, sum as spark_sum, trim, when

from shared.path_utils import DATA_INPUT
from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-10-groupby-report-demo")

    try:
        customer_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(DATA_INPUT / "customer.csv"))
            .filter(col("customer_id").isNotNull())
            .withColumn("province_code", trim(col("province_code")))
            .withColumn("amount", col("amount").cast("double"))
            .filter(col("amount").isNotNull() & (col("amount") > 0))
            .withColumn(
                "amount_level",
                when(col("amount") >= 1000000, "HIGH")
                .when(col("amount") >= 500000, "MEDIUM")
                .otherwise("LOW"),
            )
        )

        province_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(DATA_INPUT / "province.csv"))
            .withColumn("province_code", trim(col("province_code")))
        )

        enriched_df = customer_df.join(province_df, on="province_code", how="left")

        report_df = (
            enriched_df
            .groupBy("province_name")
            .agg(
                count("*").alias("customer_count"),
                spark_sum("amount").alias("total_amount"),
                avg("amount").alias("avg_amount"),
                max("amount").alias("max_amount"),
                min("amount").alias("min_amount"),
            )
            .orderBy(col("total_amount").desc())
        )

        print("=== REPORT BY PROVINCE ===")
        report_df.show(truncate=False)

        print("=== REPORT SCHEMA ===")
        report_df.printSchema()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
