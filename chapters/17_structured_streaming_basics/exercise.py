import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, when

from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-17-structured-streaming-basics-exercise")

    try:
        rate_df = (
            spark.readStream
            .format("rate")
            .option("rowsPerSecond", 3)
            .load()
        )

        event_df = (
            rate_df
            .withColumn(
                "event_type",
                when((col("value") % 2) == 0, "EVEN_EVENT").otherwise("ODD_EVENT"),
            )
        )

        query = (
            event_df.writeStream
            .format("console")
            .outputMode("append")
            .option("truncate", False)
            .trigger(processingTime="5 seconds")
            .start()
        )

        query.awaitTermination(10)
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
