import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, from_json, to_json
from pyspark.sql.types import DecimalType, StringType, StructField, StructType

from shared.spark_utils import create_spark_session


spark = create_spark_session("chapter-05-semi-structured-data-exercise")

order_schema = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("amount", DecimalType(10, 2), nullable=True),
])

orders_df = spark.createDataFrame(
    [
        ('{"order_id":"O001","amount":125000.50}',),
        ('{"order_id":"O002","amount":89000.00}',),
    ],
    ["json_text"],
)

parsed_orders_df = orders_df.select(
    from_json(col("json_text"), order_schema).alias("order")
)

parsed_orders_df.show(truncate=False)
parsed_orders_df.printSchema()
parsed_orders_df.select(to_json(col("order")).alias("json_text")).show(truncate=False)

spark.stop()
