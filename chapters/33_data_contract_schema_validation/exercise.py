import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from shared.spark_utils import create_spark_session


EXPECTED_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("province_code", StringType(), True),
        StructField("amount", DoubleType(), True),
    ]
)


def validate_schema(actual_schema: StructType, expected_schema: StructType) -> list[str]:
    actual_fields = {field.name: field.dataType for field in actual_schema.fields}
    expected_fields = {field.name: field.dataType for field in expected_schema.fields}

    errors = []
    for column_name, expected_type in expected_fields.items():
        if column_name not in actual_fields:
            errors.append(f"Missing column: {column_name}")
        elif actual_fields[column_name] != expected_type:
            errors.append(
                f"Wrong type for {column_name}: "
                f"expected {expected_type.simpleString()}, actual {actual_fields[column_name].simpleString()}"
            )
    return errors


def main() -> None:
    spark = create_spark_session("chapter-33-data-contract-schema-validation-exercise")

    try:
        actual_df = spark.createDataFrame(
            [
                ("001", "01", "100000"),
                ("002", "79", "250000"),
            ],
            schema=StructType(
                [
                    StructField("customer_id", StringType(), True),
                    StructField("province_code", StringType(), True),
                    StructField("amount", StringType(), True),
                ]
            ),
        )

        errors = validate_schema(actual_df.schema, EXPECTED_SCHEMA)

        print("=== ACTUAL SCHEMA ===")
        actual_df.printSchema()

        print("=== SCHEMA VALIDATION RESULT ===")
        if errors:
            for error in errors:
                print(error)
            print("Input schema does not match data contract.")
            return

        print("Schema is OK.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
