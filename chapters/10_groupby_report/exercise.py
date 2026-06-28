import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import avg, col, count, sum as spark_sum, trim, when

from shared.path_utils import DATA_INPUT
from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-10-groupby-report-exercise")

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
                "customer_segment",
                when(col("amount") >= 1000000, "VIP")
                .when(col("amount") >= 500000, "STANDARD")
                .otherwise("BASIC"),
            )
        )

        province_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(DATA_INPUT / "province.csv"))
            .withColumn("province_code", trim(col("province_code")))
        )

        report_df = (
            customer_df
            .join(province_df, on="province_code", how="left")
            .groupBy("province_name", "customer_segment")
            .agg(
                count("*").alias("customer_count"),
                spark_sum("amount").alias("total_amount"),
                avg("amount").alias("avg_amount"),
            )
            .orderBy(col("province_name").asc_nulls_last(), col("total_amount").desc())
        )

        print(f"report_rows: {report_df.count()}")

        print("=== REPORT BY PROVINCE AND SEGMENT ===")
        report_df.show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
