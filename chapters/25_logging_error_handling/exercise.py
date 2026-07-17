import logging
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, expr, trim, when

from shared.spark_utils import create_spark_session


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("chapter-25-exercise")


def main() -> None:
    spark = create_spark_session("chapter-25-logging-error-handling-exercise")

    try:
        logger.info("Start transaction validation job")

        transaction_df = spark.createDataFrame(
            [
                ("T001", "C001", "50000"),
                ("T002", "C002", "bad_amount"),
                ("T003", "", "70000"),
                ("T004", "C004", "90000"),
            ],
            ["transaction_id", "customer_id", "amount"],
        )

        logger.info("Input records: %s", transaction_df.count())

        validated_df = (
            transaction_df
            .withColumn("transaction_id", trim(col("transaction_id")))
            .withColumn("customer_id", trim(col("customer_id")))
            .withColumn("amount_number", expr("try_cast(amount as double)"))
            .withColumn(
                "quality_status",
                when(col("transaction_id").isNull() | (col("transaction_id") == ""), "MISSING_TRANSACTION_ID")
                .when(col("customer_id").isNull() | (col("customer_id") == ""), "MISSING_CUSTOMER_ID")
                .when(col("amount_number").isNull() | (col("amount_number") <= 0), "INVALID_AMOUNT")
                .otherwise("OK"),
            )
        )

        valid_df = validated_df.filter(col("quality_status") == "OK")
        invalid_df = validated_df.filter(col("quality_status") != "OK")

        valid_count = valid_df.count()
        invalid_count = invalid_df.count()

        logger.info("Output valid records: %s", valid_count)
        logger.warning("Validation errors: %s", invalid_count)

        print("=== INVALID RECORDS ===")
        invalid_df.show(truncate=False)

        if valid_count == 0:
            raise ValueError("No valid records to load")

        logger.info("End transaction validation job")
    except Exception:
        logger.exception("Transaction validation job failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
