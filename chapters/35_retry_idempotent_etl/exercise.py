import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import lit

from shared.path_utils import DATA_INPUT, DATA_OUTPUT
from shared.spark_utils import create_spark_session


def retry(function, max_attempts: int = 3, sleep_seconds: int = 1):
    for attempt in range(1, max_attempts + 1):
        try:
            return function()
        except RuntimeError as error:
            print(f"Attempt {attempt} failed: {error}")
            if attempt == max_attempts:
                raise
            time.sleep(sleep_seconds)


def main() -> None:
    run_date = "2026-07-20"
    spark = create_spark_session("chapter-35-retry-idempotent-etl-exercise")

    try:
        state = {"attempts": 0}

        def read_customer():
            state["attempts"] += 1
            if state["attempts"] == 1:
                raise RuntimeError("Simulated temporary read error.")
            return (
                spark.read
                .option("header", True)
                .option("inferSchema", False)
                .csv(str(DATA_INPUT / "customer.csv"))
            )

        customer_df = retry(read_customer)
        output_path = DATA_OUTPUT / "chapter35" / "exercise" / f"run_date={run_date}"

        result_df = customer_df.withColumn("run_date", lit(run_date))

        print("=== EXERCISE RESULT ===")
        result_df.show(truncate=False)
        print("=== SAFE RERUN WRITE PLAN ===")
        print(f"Use overwrite path: {output_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
