import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from shared.path_utils import DATA_INPUT
from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-17-structured-streaming-basics-demo")

    try:
        schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("transaction_date", StringType(), True),
            StructField("amount", DoubleType(), True),
        ])

        stream_df = (
            spark.readStream
            .schema(schema)
            .option("header", True)
            .csv(str(DATA_INPUT / "streaming_transactions"))
        )

        transaction_stream_df = stream_df.filter("transaction_id IS NOT NULL")

        query = (
            transaction_stream_df.writeStream
            .format("console")
            .outputMode("append")
            .option("truncate", False)
            .trigger(once=True)
            .start()
        )

        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
