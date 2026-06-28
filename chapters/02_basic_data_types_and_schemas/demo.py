import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from shared.spark_utils import create_spark_session


spark = create_spark_session("chapter-02-basic-data-types-and-schemas")

schema = StructType([
    # Identifier is a string so values such as "001" keep leading zeros.
    StructField("customer_id", StringType(), nullable=False),
    StructField("age", IntegerType(), nullable=False),
    StructField("conversion_rate", FloatType(), nullable=True),
    StructField("quality_score", DoubleType(), nullable=True),
    # Decimal is preferable to float/double for exact monetary values.
    StructField("account_balance", DecimalType(12, 2), nullable=False),
    StructField("is_active", BooleanType(), nullable=False),
    StructField("registered_date", DateType(), nullable=False),
    StructField("last_login_at", TimestampType(), nullable=True),
])

data = [
    ("001", 30, 0.25, 98.123456789, Decimal("150000.50"), True,
     date(2024, 1, 15), datetime(2026, 6, 22, 9, 30)),
    ("002", 25, None, 87.987654321, Decimal("250000.00"), False,
     date(2024, 2, 20), None),
]

customer_df = spark.createDataFrame(data, schema=schema)

print("=== EXPLICIT SCHEMA ===")
customer_df.printSchema()

print("=== DATA WITH BASIC TYPES ===")
customer_df.show(truncate=False)

print("customer_id remains a string, including its leading zeros.")
print("account_balance uses DecimalType for exact monetary values.")

spark.stop()
