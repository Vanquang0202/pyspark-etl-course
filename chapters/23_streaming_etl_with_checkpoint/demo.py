import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, when

from shared.path_utils import DATA_OUTPUT
from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-23-streaming-etl-with-checkpoint-demo")

    try:
        rate_df = (
            spark.readStream
            .format("rate")
            .option("rowsPerSecond", 2)
            .load()
        )

        event_df = (
            rate_df
            .withColumn("event_id", col("value"))
            .withColumn(
                "event_type",
                when((col("value") % 2) == 0, "EVEN").otherwise("ODD"),
            )
            .select("timestamp", "event_id", "event_type")
        )

        checkpoint_path = str(DATA_OUTPUT / "chapter23" / "checkpoint_demo")

        query = (
            event_df.writeStream
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
