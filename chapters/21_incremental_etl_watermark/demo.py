import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, lit, max as spark_max, to_timestamp, upper

from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-21-incremental-etl-watermark-demo")

    try:
        last_watermark = "2026-07-08 10:00:00"

        source_df = spark.createDataFrame(
            [
                ("C001", "an", "2026-07-08 09:30:00"),
                ("C002", "binh", "2026-07-08 10:15:00"),
                ("C003", "cuong", "2026-07-08 11:20:00"),
            ],
            ["customer_id", "customer_name", "updated_at_text"],
        ).withColumn("updated_at", to_timestamp(col("updated_at_text")))

        new_rows_df = source_df.filter(col("updated_at") > to_timestamp(lit(last_watermark)))

        transformed_df = (
            new_rows_df
            .withColumn("customer_name", upper(col("customer_name")))
            .select("customer_id", "customer_name", "updated_at")
        )

        new_watermark_row = transformed_df.agg(spark_max("updated_at").alias("new_watermark")).first()
        new_watermark = new_watermark_row["new_watermark"] if new_watermark_row else None

        print(f"last_watermark: {last_watermark}")
        print("=== NEW OR CHANGED ROWS ===")
        transformed_df.show(truncate=False)
        print(f"new_watermark: {new_watermark}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
