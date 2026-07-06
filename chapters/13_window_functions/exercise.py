import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, sum as spark_sum, to_date

from shared.path_utils import DATA_INPUT
from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-13-window-functions-exercise")

    try:
        transaction_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(DATA_INPUT / "customer_transactions.csv"))
            .withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))
            .withColumn("amount", col("amount").cast("double"))
        )

        latest_window = (
            Window
            .partitionBy("customer_id")
            .orderBy(col("transaction_date").desc(), col("transaction_id").desc())
        )
        customer_window = Window.partitionBy("customer_id")

        report_df = (
            transaction_df
            .withColumn("latest_rank", row_number().over(latest_window))
            .withColumn("customer_total_amount", spark_sum("amount").over(customer_window))
            .filter(col("latest_rank") == 1)
            .select(
                "customer_id",
                col("transaction_id").alias("latest_transaction_id"),
                col("transaction_date").alias("latest_transaction_date"),
                col("amount").alias("latest_amount"),
                "customer_total_amount",
            )
            .orderBy("customer_id")
        )

        print("=== LATEST CUSTOMER TRANSACTION REPORT ===")
        report_df.show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
