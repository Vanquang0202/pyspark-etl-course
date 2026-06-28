import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, trim, when

from shared.path_utils import DATA_INPUT
from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-09-join-mapping-check-demo")

    try:
        customer_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(DATA_INPUT / "customer.csv"))
            .withColumn("name", trim(col("name")))
            .withColumn("province_code", trim(col("province_code")))
        )

        province_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(DATA_INPUT / "province.csv"))
            .withColumn("province_code", trim(col("province_code")))
        )

        print("=== CUSTOMER SCHEMA ===")
        customer_df.printSchema()

        print("=== PROVINCE SCHEMA ===")
        province_df.printSchema()

        inner_join_df = customer_df.join(province_df, on="province_code", how="inner")

        print("=== INNER JOIN RESULT ===")
        inner_join_df.show(truncate=False)

        left_join_df = customer_df.join(province_df, on="province_code", how="left")

        mapped_result_df = (
            left_join_df
            .withColumn(
                "mapping_status",
                when(col("province_name").isNotNull(), "MAPPED").otherwise("UNMAPPED"),
            )
            .select(
                "customer_id",
                "name",
                "province_code",
                "province_name",
                "mapping_status",
            )
        )

        print("=== LEFT JOIN WITH MAPPING STATUS ===")
        mapped_result_df.show(truncate=False)

        total_customers = mapped_result_df.count()
        mapped_customers = mapped_result_df.filter(col("mapping_status") == "MAPPED").count()
        unmapped_customers = mapped_result_df.filter(col("mapping_status") == "UNMAPPED").count()

        print(f"total_customers: {total_customers}")
        print(f"mapped_customers: {mapped_customers}")
        print(f"unmapped_customers: {unmapped_customers}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
