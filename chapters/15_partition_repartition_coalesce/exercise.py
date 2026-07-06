import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import current_date, trim, col

from shared.path_utils import DATA_INPUT, DATA_OUTPUT
from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-15-partition-repartition-coalesce-exercise")

    try:
        customer_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(DATA_INPUT / "customer.csv"))
            .withColumn("province_code", trim(col("province_code")))
            .withColumn("processing_date", current_date())
        )

        print(f"initial_partitions: {customer_df.rdd.getNumPartitions()}")

        output_df = customer_df.repartition(2, "processing_date")

        print(f"output_partitions: {output_df.rdd.getNumPartitions()}")

        output_path = str(DATA_OUTPUT / "chapter15" / "customer_by_processing_date")
        (
            output_df.write
            .mode("overwrite")
            .partitionBy("processing_date")
            .csv(output_path, header=True)
        )

        print(f"Wrote partitioned output to: {output_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
