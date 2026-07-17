import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when

sys.path.append(str(Path(__file__).resolve().parents[2]))


def cast_transaction_df(transaction_df):
    return (
        transaction_df
        .withColumn("transaction_id", trim(col("transaction_id")))
        .withColumn("customer_id", trim(col("customer_id")))
        .withColumn("amount", col("amount").cast("double"))
    )


def validate_transaction_df(transaction_df):
    return transaction_df.withColumn(
        "quality_status",
        when(col("transaction_id").isNull() | (col("transaction_id") == ""), "MISSING_TRANSACTION_ID")
        .when(col("customer_id").isNull() | (col("customer_id") == ""), "MISSING_CUSTOMER_ID")
        .when(col("amount").isNull() | (col("amount") <= 0), "INVALID_AMOUNT")
        .otherwise("OK"),
    )


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder
        .master("local[1]")
        .appName("chapter-24-pyspark-unit-testing-exercise")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


def test_cast_transaction_df(spark):
    input_df = spark.createDataFrame(
        [(" T001 ", " C001 ", "150000")],
        ["transaction_id", "customer_id", "amount"],
    )

    result = cast_transaction_df(input_df).collect()[0]

    assert result["transaction_id"] == "T001"
    assert result["customer_id"] == "C001"
    assert result["amount"] == 150000.0


def test_validate_transaction_df_detects_missing_customer_id(spark):
    input_df = spark.createDataFrame(
        [("T002", "", 100000.0)],
        ["transaction_id", "customer_id", "amount"],
    )

    result = validate_transaction_df(input_df).collect()[0]

    assert result["quality_status"] == "MISSING_CUSTOMER_ID"


def test_validate_transaction_df_detects_ok_record(spark):
    input_df = spark.createDataFrame(
        [("T003", "C003", 200000.0)],
        ["transaction_id", "customer_id", "amount"],
    )

    result = validate_transaction_df(input_df).collect()[0]

    assert result["quality_status"] == "OK"
