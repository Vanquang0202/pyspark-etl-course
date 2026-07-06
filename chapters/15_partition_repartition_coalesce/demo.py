import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, trim

from shared.path_utils import DATA_INPUT, DATA_OUTPUT
from shared.spark_utils import create_spark_session


def print_partitions(label: str, df) -> None:
    print(f"{label}: {df.rdd.getNumPartitions()}")


def main() -> None:
    spark = create_spark_session("chapter-15-partition-repartition-coalesce-demo")

    try:
        customer_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(DATA_INPUT / "customer.csv"))
            .withColumn("province_code", trim(col("province_code")))
        )

        print_partitions("initial_partitions", customer_df)

        repartitioned_df = customer_df.repartition(4, "province_code")
        print_partitions("after_repartition_by_province", repartitioned_df)

        coalesced_df = repartitioned_df.coalesce(1)
        print_partitions("after_coalesce", coalesced_df)

        output_path = str(DATA_OUTPUT / "chapter15" / "customer_by_province")
        (
            repartitioned_df.write
            .mode("overwrite")
            .partitionBy("province_code")
            .csv(output_path, header=True)
        )

        print(f"Wrote partitioned output to: {output_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
