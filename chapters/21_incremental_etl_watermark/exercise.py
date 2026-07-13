import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, expr, lit, max as spark_max, to_timestamp, trim, when

from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-21-incremental-etl-watermark-exercise")

    try:
        last_watermark = "2026-07-08 12:00:00"

        source_df = spark.createDataFrame(
            [
                (1001, "C001", " 100000 ", "2026-07-08 11:55:00"),
                (1002, "C002", "250000", "2026-07-08 12:05:00"),
                (1003, "C003", "bad", "2026-07-08 12:10:00"),
                (1004, "C004", "300000", "2026-07-08 12:20:00"),
            ],
            ["source_id", "customer_id", "amount_text", "updated_at_text"],
        ).withColumn("updated_at", to_timestamp(col("updated_at_text")))

        incremental_df = source_df.filter(col("updated_at") > to_timestamp(lit(last_watermark)))

        transformed_df = (
            incremental_df
            .withColumn("amount_clean", trim(col("amount_text")))
            .withColumn("amount", expr("try_cast(amount_clean as double)"))
            .withColumn(
                "quality_status",
                when(col("amount").isNull() | (col("amount") <= 0), "INVALID_AMOUNT")
                .otherwise("OK"),
            )
            .select("source_id", "customer_id", "amount", "quality_status", "updated_at")
        )

        new_watermark = transformed_df.agg(spark_max("updated_at").alias("new_watermark")).first()[
            "new_watermark"
        ]

        print(f"last_watermark: {last_watermark}")
        print("=== INCREMENTAL ROWS AFTER TRANSFORM ===")
        transformed_df.show(truncate=False)
        print(f"new_watermark: {new_watermark}")
        print("Sau khi ghi target thanh cong moi nen update bang ETL_WATERMARK.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
