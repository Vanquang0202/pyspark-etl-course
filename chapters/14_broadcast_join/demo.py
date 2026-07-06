import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import broadcast, col, trim

from shared.path_utils import DATA_INPUT
from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-14-broadcast-join-demo")

    try:
        customer_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(DATA_INPUT / "customer.csv"))
            .withColumn("province_code", trim(col("province_code")))
        )

        province_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(DATA_INPUT / "province.csv"))
            .withColumn("province_code", trim(col("province_code")))
        )

        normal_join_df = customer_df.join(province_df, on="province_code", how="left")
        broadcast_join_df = customer_df.join(broadcast(province_df), on="province_code", how="left")

        print("=== NORMAL JOIN RESULT ===")
        normal_join_df.select("customer_id", "name", "province_code", "province_name").show(truncate=False)

        print("=== BROADCAST JOIN RESULT ===")
        broadcast_join_df.select("customer_id", "name", "province_code", "province_name").show(truncate=False)

        print("=== BROADCAST JOIN EXPLAIN PLAN ===")
        broadcast_join_df.explain()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
