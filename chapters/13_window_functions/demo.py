import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql import Window
from pyspark.sql.functions import col, dense_rank, lag, lead, rank, row_number, sum as spark_sum, to_date

from shared.path_utils import DATA_INPUT
from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-13-window-functions-demo")

    try:
        transaction_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(DATA_INPUT / "customer_transactions.csv"))
            .withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))
            .withColumn("amount", col("amount").cast("double"))
        )

        by_customer_latest = (
            Window
            .partitionBy("customer_id")
            .orderBy(col("transaction_date").desc(), col("transaction_id").desc())
        )

        by_customer_amount = (
            Window
            .partitionBy("customer_id")
            .orderBy(col("amount").desc())
        )

        by_customer_date = (
            Window
            .partitionBy("customer_id")
            .orderBy("transaction_date")
        )

        by_customer = Window.partitionBy("customer_id")

        window_df = (
            transaction_df
            .withColumn("row_number_latest", row_number().over(by_customer_latest))
            .withColumn("rank_by_amount", rank().over(by_customer_amount))
            .withColumn("dense_rank_by_amount", dense_rank().over(by_customer_amount))
            .withColumn("previous_amount", lag("amount").over(by_customer_date))
            .withColumn("next_amount", lead("amount").over(by_customer_date))
            .withColumn("amount_change", col("amount") - col("previous_amount"))
            .withColumn("customer_total_amount", spark_sum("amount").over(by_customer))
        )

        print("=== WINDOW RESULT ===")
        window_df.orderBy("customer_id", "transaction_date").show(truncate=False)

        print("=== LATEST TRANSACTION PER CUSTOMER ===")
        (
            window_df
            .filter(col("row_number_latest") == 1)
            .select(
                "customer_id",
                "transaction_id",
                "transaction_date",
                "amount",
                "customer_total_amount",
            )
            .orderBy("customer_id")
            .show(truncate=False)
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
