import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, initcap, trim, when

sys.path.append(str(Path(__file__).resolve().parents[2]))


def clean_customer_df(customer_df):
    return (
        customer_df
        .withColumn("customer_id", trim(col("customer_id")))
        .withColumn("customer_name", initcap(trim(col("customer_name"))))
        .withColumn("amount", col("amount").cast("double"))
    )


def validate_customer_df(customer_df):
    return customer_df.withColumn(
        "quality_status",
        when(col("customer_id").isNull() | (col("customer_id") == ""), "MISSING_CUSTOMER_ID")
        .when(col("customer_name").isNull() | (col("customer_name") == ""), "MISSING_NAME")
        .when(col("amount").isNull() | (col("amount") <= 0), "INVALID_AMOUNT")
        .otherwise("OK"),
    )


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder
        .master("local[1]")
        .appName("chapter-24-pyspark-unit-testing-demo")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


def test_clean_customer_df(spark):
    input_df = spark.createDataFrame(
        [(" C001 ", " nguyen van an ", "100000")],
        ["customer_id", "customer_name", "amount"],
    )

    result = clean_customer_df(input_df).collect()[0]

    assert result["customer_id"] == "C001"
    assert result["customer_name"] == "Nguyen Van An"
    assert result["amount"] == 100000.0


def test_validate_customer_df_returns_ok(spark):
    input_df = spark.createDataFrame(
        [("C001", "Nguyen Van An", 100000.0)],
        ["customer_id", "customer_name", "amount"],
    )

    result = validate_customer_df(input_df).collect()[0]

    assert result["quality_status"] == "OK"


def test_validate_customer_df_detects_invalid_amount(spark):
    input_df = spark.createDataFrame(
        [("C002", "Tran Thi Binh", -50.0)],
        ["customer_id", "customer_name", "amount"],
    )

    result = validate_customer_df(input_df).collect()[0]

    assert result["quality_status"] == "INVALID_AMOUNT"
