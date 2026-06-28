import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, from_json, to_json, try_parse_json, try_variant_get
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from shared.path_utils import DATA_INPUT
from shared.spark_utils import create_spark_session


spark = create_spark_session("chapter-05-semi-structured-data")

print("=== READ JSON FILE ===")
json_file_df = spark.read.json(str(DATA_INPUT / "customer_profiles.json"))
json_file_df.show(truncate=False)
json_file_df.printSchema()

profile_schema = StructType([
    StructField("name", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=True),
])

json_strings_df = spark.createDataFrame(
    [('{"name":"Le Van C","age":35}',), ('{"name":"Pham Thi D","age":28}',)],
    ["json_text"],
)

parsed_df = json_strings_df.select(
    from_json(col("json_text"), profile_schema).alias("profile")
)

print("=== FROM_JSON ===")
parsed_df.show(truncate=False)
parsed_df.printSchema()

print("=== TO_JSON ===")
parsed_df.select(to_json(col("profile")).alias("json_text")).show(truncate=False)

print("=== READ XML FILE (SPARK 4.0+) ===")
xml_df = (
    spark.read
    .format("xml")
    .option("rowTag", "customer")
    .load(str(DATA_INPUT / "customer_profiles.xml"))
)
xml_df.show(truncate=False)
xml_df.printSchema()

print("=== VARIANT (SPARK 4.0+) ===")
variant_source_df = spark.createDataFrame(
    [('{"id":1,"channel":"email"}',), ('{"id":2,"channel":"sms"}',)],
    ["json_text"],
)
variant_df = variant_source_df.select(
    try_parse_json(col("json_text")).alias("variant_data")
)
variant_df.printSchema()
variant_df.select(
    try_variant_get(col("variant_data"), "$.id", "int").alias("id"),
    try_variant_get(col("variant_data"), "$.channel", "string").alias("channel"),
).show(truncate=False)

spark.stop()
