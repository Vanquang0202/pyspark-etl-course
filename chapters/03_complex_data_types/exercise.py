import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql import Row
from pyspark.sql.types import ArrayType, MapType, StringType, StructField, StructType

from shared.spark_utils import create_spark_session


spark = create_spark_session("chapter-03-complex-data-types-exercise")

contact_schema = StructType([
    StructField("email", StringType(), nullable=False),
    StructField("phone", StringType(), nullable=True),
])

employee_schema = StructType([
    StructField("employee_id", StringType(), nullable=False),
    StructField("skills", ArrayType(StringType()), nullable=False),
    StructField("contact", contact_schema, nullable=False),
    StructField("metadata", MapType(StringType(), StringType()), nullable=True),
])

employees = [
    Row(
        employee_id="E001",
        skills=["PySpark", "SQL"],
        contact=Row(email="employee@example.com", phone=None),
        metadata={"level": "junior", "team": "data"},
    )
]

employee_df = spark.createDataFrame(employees, schema=employee_schema)

employee_df.show(truncate=False)
employee_df.printSchema()
employee_df.select(
    "employee_id",
    employee_df.contact.email.alias("email"),
    employee_df.metadata["team"].alias("team"),
).show(truncate=False)

spark.stop()
