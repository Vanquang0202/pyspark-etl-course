import argparse
import json
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import lit

from shared.path_utils import PROJECT_ROOT
from shared.spark_utils import create_spark_session


VALID_ENVS = {"dev", "test", "prod"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Demo environment config for ETL.")
    parser.add_argument("--env", choices=sorted(VALID_ENVS), help="Environment name.")
    return parser.parse_args()


def select_env(argument_env: str | None) -> str:
    env = argument_env or os.environ.get("ETL_ENV", "dev")
    if env not in VALID_ENVS:
        raise ValueError(f"Invalid env '{env}'. Expected one of: {sorted(VALID_ENVS)}")
    return env


def load_config(env: str) -> dict:
    config_path = Path(__file__).resolve().parent / f"config_{env}.json"
    with config_path.open("r", encoding="utf-8") as file:
        return json.load(file)


def main() -> None:
    args = parse_args()
    env = select_env(args.env)
    config = load_config(env)
    spark = create_spark_session("chapter-32-environment-config-demo")

    try:
        customer_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(PROJECT_ROOT / config["input_path"]))
        )

        result_df = customer_df.withColumn("env", lit(config["env"])).withColumn("run_date", lit(config["run_date"]))

        print("=== ACTIVE CONFIG ===")
        print(config)
        print("=== RESULT PREVIEW ===")
        result_df.show(truncate=False)
        print("=== WRITE PLAN ===")
        print(f"mode={config['write_mode']}")
        print(f"output_path={PROJECT_ROOT / config['output_path']}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
