import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, lit, trim, when

from shared.path_utils import PROJECT_ROOT
from shared.spark_utils import create_spark_session


def load_config(config_path: Path) -> dict:
    with config_path.open("r", encoding="utf-8") as file:
        return json.load(file)


def main() -> None:
    spark = create_spark_session("chapter-26-config-driven-etl-exercise")

    try:
        config = load_config(Path(__file__).resolve().parent / "config.dev.json")

        customer_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(PROJECT_ROOT / config["input_path"]))
        )

        validated_df = (
            customer_df
            .withColumn("customer_id", trim(col("customer_id")))
            .withColumn("province_code", trim(col("province_code")))
            .withColumn("amount_number", col("amount").cast("double"))
            .withColumn("run_date", lit(config["run_date"]))
            .withColumn(
                "quality_status",
                when(col("customer_id").isNull() | (col("customer_id") == ""), "MISSING_CUSTOMER_ID")
                .when(col("province_code").isNull() | (col("province_code") == ""), "MISSING_PROVINCE_CODE")
                .when(col("amount_number").isNull() | (col("amount_number") <= 0), "INVALID_AMOUNT")
                .otherwise("OK"),
            )
        )

        print("=== VALIDATED DATA ===")
        validated_df.show(truncate=False)

        print("=== CONFIG VALUES USED BY JOB ===")
        print(f"input_path={PROJECT_ROOT / config['input_path']}")
        print(f"output_path={PROJECT_ROOT / config['output_path']}")
        print(f"run_date={config['run_date']}")
        print(f"write_mode={config['write_mode']}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
