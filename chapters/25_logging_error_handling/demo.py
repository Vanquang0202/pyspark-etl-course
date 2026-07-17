import logging
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, initcap, trim, when

from shared.spark_utils import create_spark_session


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("chapter-25-demo")


def main() -> None:
    spark = create_spark_session("chapter-25-logging-error-handling-demo")

    try:
        logger.info("Start ETL job")

        source_df = spark.createDataFrame(
            [
                ("C001", " nguyen van an ", "100000"),
                ("C002", "tran thi binh", "-50"),
                ("", "le van cuong", "200000"),
            ],
            ["customer_id", "customer_name", "amount"],
        )

        input_count = source_df.count()
        logger.info("Input records: %s", input_count)

        cleaned_df = (
            source_df
            .withColumn("customer_id", trim(col("customer_id")))
            .withColumn("customer_name", initcap(trim(col("customer_name"))))
            .withColumn("amount", col("amount").cast("double"))
        )

        validated_df = cleaned_df.withColumn(
            "quality_status",
            when(col("customer_id").isNull() | (col("customer_id") == ""), "MISSING_CUSTOMER_ID")
            .when(col("amount").isNull() | (col("amount") <= 0), "INVALID_AMOUNT")
            .otherwise("OK"),
        )

        valid_df = validated_df.filter(col("quality_status") == "OK")
        invalid_df = validated_df.filter(col("quality_status") != "OK")

        valid_count = valid_df.count()
        invalid_count = invalid_df.count()

        logger.info("Valid records: %s", valid_count)
        logger.warning("Invalid records: %s", invalid_count)

        if invalid_count > 0:
            logger.warning("There are invalid records. Check quality_status before loading target.")
            invalid_df.show(truncate=False)

        print("=== VALID RECORDS ===")
        valid_df.show(truncate=False)

        logger.info("End ETL job")
    except Exception:
        logger.exception("ETL job failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
