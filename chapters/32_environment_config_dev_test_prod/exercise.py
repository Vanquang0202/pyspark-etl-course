import argparse
import json
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import col, lit, trim

from shared.path_utils import PROJECT_ROOT
from shared.spark_utils import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Exercise environment config for ETL.")
    parser.add_argument("--env", choices=["dev", "test", "prod"])
    return parser.parse_args()


def load_config(env: str) -> dict:
    config_path = Path(__file__).resolve().parent / f"config_{env}.json"
    with config_path.open("r", encoding="utf-8") as file:
        return json.load(file)


def main() -> None:
    args = parse_args()
    env = args.env or os.environ.get("ETL_ENV", "dev")
    config = load_config(env)
    spark = create_spark_session("chapter-32-environment-config-exercise")

    try:
        customer_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(PROJECT_ROOT / config["input_path"]))
        )

        cleaned_df = (
            customer_df
            .withColumn("customer_id", trim(col("customer_id")))
            .withColumn("province_code", trim(col("province_code")))
            .withColumn("run_date", lit(config["run_date"]))
            .withColumn("env", lit(config["env"]))
        )

        print("=== CLEANED DATA ===")
        cleaned_df.show(truncate=False)
        print("=== CONFIG SOURCE ===")
        print(f"env={env}")
        print(f"config_file=config_{env}.json")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
