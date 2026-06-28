import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql import Row
from pyspark.sql.functions import col, trim, when

from shared.path_utils import DATA_INPUT
from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-09-join-mapping-check-exercise")

    try:
        customer_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(DATA_INPUT / "customer.csv"))
            .filter(col("customer_id").isNotNull())
            .withColumn("name", trim(col("name")))
            .withColumn("province_code", trim(col("province_code")))
        )

        extra_customer_df = spark.createDataFrame(
            [
                Row(
                    customer_id="900",
                    name="Mapping Error One",
                    gender="F",
                    birth_date="01/01/1990",
                    province_code="88",
                    amount="100000",
                ),
                Row(
                    customer_id="901",
                    name="Mapping Error Two",
                    gender="M",
                    birth_date="01/01/1991",
                    province_code="00",
                    amount="200000",
                ),
            ],
            schema=customer_df.schema,
        )

        customer_with_error_df = customer_df.unionByName(extra_customer_df)

        province_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(DATA_INPUT / "province.csv"))
            .withColumn("province_code", trim(col("province_code")))
        )

        result_df = (
            customer_with_error_df
            .join(province_df, on="province_code", how="left")
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

        mapped_df = result_df.filter(col("mapping_status") == "MAPPED")
        unmapped_df = result_df.filter(col("mapping_status") == "UNMAPPED")

        print(f"total_records: {result_df.count()}")
        print(f"mapped_records: {mapped_df.count()}")
        print(f"unmapped_records: {unmapped_df.count()}")

        print("=== MAPPED RECORDS ===")
        mapped_df.show(truncate=False)

        print("=== UNMAPPED RECORDS ===")
        unmapped_df.show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
