import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql import Row
from pyspark.sql.types import (
    ArrayType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from shared.spark_utils import create_spark_session


spark = create_spark_session("chapter-03-complex-data-types")

address_schema = StructType([
    StructField("street", StringType(), nullable=False),
    StructField("city", StringType(), nullable=False),
    StructField("zip_code", StringType(), nullable=False),
])

customer_schema = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("addresses", ArrayType(address_schema), nullable=True),
    StructField(
        "preferences",
        MapType(StringType(), StringType()),
        nullable=True,
    ),
])

customers = [
    Row(
        customer_id="001",
        name="Nguyen Van A",
        addresses=[
            Row(street="1 Main Street", city="Ha Noi", zip_code="10000"),
            Row(street="9 Lake Road", city="Da Nang", zip_code="50000"),
        ],
        preferences={"language": "vi", "channel": "email"},
    ),
    Row(
        customer_id="002",
        name="Tran Thi B",
        addresses=[
            Row(street="2 Central Street", city="Ho Chi Minh", zip_code="70000"),
        ],
        preferences={"language": "en", "channel": "sms"},
    ),
]

customer_df = spark.createDataFrame(customers, schema=customer_schema)

print("=== COMPLEX DATA ===")
customer_df.show(truncate=False)
customer_df.printSchema()

print("=== FIRST CITY AND PREFERRED CHANNEL ===")
customer_df.select(
    "customer_id",
    customer_df.addresses[0].city.alias("first_city"),
    customer_df.preferences["channel"].alias("preferred_channel"),
).show(truncate=False)

spark.stop()
