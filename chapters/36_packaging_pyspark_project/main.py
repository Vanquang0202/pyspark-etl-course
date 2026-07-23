import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from etl_package.transforms import add_quality_status, clean_customers
from shared.path_utils import DATA_INPUT
from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-36-packaging-pyspark-project-main")

    try:
        customer_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(DATA_INPUT / "customer.csv"))
        )

        result_df = add_quality_status(clean_customers(customer_df))

        print("=== PACKAGED ETL RESULT ===")
        result_df.show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
