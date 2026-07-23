"""Spark Structured Streaming consumer cho customer-events."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


TOPIC = "customer-events"
EVENT_SCHEMA = StructType([
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("province_code", StringType()),
    StructField("event_time", StringType()),
])


def main() -> None:
    spark = SparkSession.builder.appName("chapter-39-kafka-consumer").getOrCreate()
    event_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", TOPIC).load()
    parsed_df = event_df.select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("event")).select("event.*")
    query = parsed_df.writeStream.format("console").option("truncate", False).option("checkpointLocation", "data/output/chapter39/checkpoint").start()
    query.awaitTermination()


if __name__ == "__main__":
    main()
