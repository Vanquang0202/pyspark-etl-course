import sys
import time
import uuid
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql.functions import lit

from shared.path_utils import DATA_INPUT, DATA_OUTPUT
from shared.spark_utils import create_spark_session


class TemporarySourceError(Exception):
    pass


def retry(function, max_attempts: int = 3, sleep_seconds: int = 1):
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            print(f"Attempt {attempt}/{max_attempts}")
            return function()
        except TemporarySourceError as error:
            last_error = error
            print(f"Temporary error: {error}")
            if attempt < max_attempts:
                time.sleep(sleep_seconds)
    raise last_error


def read_source_with_temporary_error(spark, state: dict):
    state["attempts"] += 1
    if state["attempts"] < 2:
        raise TemporarySourceError("Source is temporarily unavailable.")

    return (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(str(DATA_INPUT / "customer.csv"))
    )


def main() -> None:
    run_date = "2026-07-20"
    run_id = str(uuid.uuid4())
    spark = create_spark_session("chapter-35-retry-idempotent-etl-demo")

    try:
        state = {"attempts": 0}
        customer_df = retry(lambda: read_source_with_temporary_error(spark, state))

        result_df = customer_df.withColumn("run_date", lit(run_date)).withColumn("run_id", lit(run_id))
        output_path = DATA_OUTPUT / "chapter35" / f"run_date={run_date}"

        print("=== RESULT PREVIEW ===")
        result_df.show(truncate=False)
        print("=== IDEMPOTENT WRITE PLAN ===")
        print(f"result_df.write.mode('overwrite').parquet('{output_path}')")
        print("Overwrite theo run_date giup chay lai job khong append duplicate.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
