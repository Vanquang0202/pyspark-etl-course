import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from shared.path_utils import DATA_OUTPUT


KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "customer-events"


def create_spark_session() -> SparkSession:
    python_executable = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    spark = (
        SparkSession.builder
        .appName("chapter-22-kafka-streaming-consumer")
        .master("local[*]")
        .config("spark.pyspark.python", python_executable)
        .config("spark.pyspark.driver.python", python_executable)
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config(
            "spark.jars.repositories",
            "https://repo.maven.apache.org/maven2,https://repo1.maven.org/maven2,https://repository.apache.org/content/repositories/releases,https://maven.aliyun.com/repository/public",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> None:
    spark = create_spark_session()

    try:
        event_schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("province_code", StringType(), True),
            StructField("amount", StringType(), True),
            StructField("event_time", StringType(), True),
        ])

        kafka_df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
            .option("subscribe", TOPIC)
            .option("startingOffsets", "earliest")
            .load()
        )

        parsed_df = (
            kafka_df
            .selectExpr("CAST(value AS STRING) AS json_value")
            .withColumn("event", from_json(col("json_value"), event_schema))
            .select(
                col("event.customer_id").alias("customer_id"),
                col("event.name").alias("name"),
                col("event.province_code").alias("province_code"),
                col("event.amount").cast(DoubleType()).alias("amount"),
                col("event.event_time").alias("event_time"),
            )
            .withColumn(
                "amount_status",
                when(col("amount") >= 1_000_000, "HIGH").otherwise("NORMAL"),
            )
        )

        checkpoint_path = str(DATA_OUTPUT / "chapter_22" / "checkpoint")

        query = (
            parsed_df.writeStream
            .format("console")
            .outputMode("append")
            .option("truncate", False)
            .option("checkpointLocation", checkpoint_path)
            .start()
        )

        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
