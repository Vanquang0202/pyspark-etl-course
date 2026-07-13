import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, when

from shared.path_utils import DATA_OUTPUT
from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-23-streaming-etl-with-checkpoint-exercise")

    try:
        stream_df = (
            spark.readStream
            .format("rate")
            .option("rowsPerSecond", 3)
            .load()
        )

        clean_event_df = (
            stream_df
            .withColumn("transaction_id", col("value"))
            .withColumn("amount", (col("value") + 1) * 1000)
            .withColumn(
                "quality_status",
                when((col("value") % 5) == 0, "NEED_REVIEW").otherwise("OK"),
            )
            .select("timestamp", "transaction_id", "amount", "quality_status")
        )

        checkpoint_path = str(DATA_OUTPUT / "chapter23" / "checkpoint_exercise")

        query = (
            clean_event_df.writeStream
            .format("console")
            .outputMode("append")
            .option("truncate", False)
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime="5 seconds")
            .start()
        )

        query.awaitTermination(15)
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
