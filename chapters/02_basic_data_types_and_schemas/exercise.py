import sys
from decimal import Decimal
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    FloatType,
    StringType,
    StructField,
    StructType,
)

from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-02-basic-data-types-and-schemas-exercise")

    product_schema = StructType([
        StructField("product_id", StringType(), nullable=False),
        StructField("product_name", StringType(), nullable=False),
        StructField("price", DecimalType(10, 2), nullable=False),
        StructField("rating", DoubleType(), nullable=True),
        StructField("discount_rate", FloatType(), nullable=True),
        StructField("note", StringType(), nullable=True),
    ])

    products = [
        ("0001", "Laptop", Decimal("1999.90"), 4.812345678, 0.10, None),
        ("0002", "Keyboard", Decimal("89.50"), None, None, "New product"),
    ]

    product_df = spark.createDataFrame(products, schema=product_schema)

    print("=== PRODUCT DATA ===")
    product_df.show(truncate=False)
    print("=== PRODUCT SCHEMA ===")
    product_df.printSchema()

    spark.stop()


if __name__ == "__main__":
    main()
