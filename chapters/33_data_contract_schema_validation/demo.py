import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from shared.spark_utils import create_spark_session


EXPECTED_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("province_code", StringType(), True),
        StructField("amount", DoubleType(), True),
    ]
)


def validate_schema(actual_schema: StructType, expected_schema: StructType) -> None:
    actual_fields = {field.name: field.dataType for field in actual_schema.fields}
    expected_fields = {field.name: field.dataType for field in expected_schema.fields}

    missing_columns = [name for name in expected_fields if name not in actual_fields]
    wrong_types = [
        f"{name}: expected {expected_fields[name].simpleString()}, actual {actual_fields[name].simpleString()}"
        for name in expected_fields
        if name in actual_fields and actual_fields[name] != expected_fields[name]
    ]

    errors = []
    if missing_columns:
        errors.append(f"Missing columns: {missing_columns}")
    if wrong_types:
        errors.append(f"Wrong data types: {wrong_types}")

    if errors:
        raise ValueError("Schema validation failed. " + " | ".join(errors))


def main() -> None:
    spark = create_spark_session("chapter-33-data-contract-schema-validation-demo")

    try:
        valid_df = spark.createDataFrame(
            [
                ("001", "Nguyen Van A", "01", 100000.0),
                ("002", "Tran Thi B", "79", 250000.0),
            ],
            schema=EXPECTED_SCHEMA,
        )

        invalid_df = spark.createDataFrame(
            [
                ("001", "Nguyen Van A", "100000"),
            ],
            schema=StructType(
                [
                    StructField("customer_id", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("amount", StringType(), True),
                ]
            ),
        )

        print("=== VALID SCHEMA CHECK ===")
        validate_schema(valid_df.schema, EXPECTED_SCHEMA)
        print("Valid schema is OK.")

        print("=== INVALID SCHEMA CHECK ===")
        try:
            validate_schema(invalid_df.schema, EXPECTED_SCHEMA)
        except ValueError as error:
            print(error)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
