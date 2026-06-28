import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, current_date, initcap, to_date, trim, when

from shared.path_utils import DATA_INPUT
from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-08-data-manipulation-exercise")
    previous_ansi_setting = spark.conf.get("spark.sql.ansi.enabled")

    try:
        spark.conf.set("spark.sql.ansi.enabled", False)

        customer_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(DATA_INPUT / "customer.csv"))
        )

        input_count = customer_df.count()

        output_df = (
            customer_df
            .withColumnRenamed("name", "customer_name")
            .withColumn("customer_name", initcap(trim(col("customer_name"))))
            .withColumn("province_code", trim(col("province_code")))
            .withColumn("amount", col("amount").cast("double"))
            .withColumn("birth_date", to_date(col("birth_date"), "dd/MM/yyyy"))
            .filter(col("amount").isNotNull() & (col("amount") > 0))
            .withColumn(
                "customer_segment",
                when(col("amount") >= 1000000, "VIP")
                .when(col("amount") >= 500000, "STANDARD")
                .otherwise("BASIC"),
            )
            .withColumn("processing_date", current_date())
            .select(
                "customer_id",
                "customer_name",
                "province_code",
                "amount",
                "customer_segment",
                "processing_date",
            )
        )

        output_count = output_df.count()

        print(f"input_count: {input_count}")
        print(f"output_count: {output_count}")

        print("=== CLEAN CUSTOMER OUTPUT ===")
        output_df.show(truncate=False)
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi_setting)
        spark.stop()


if __name__ == "__main__":
    main()
