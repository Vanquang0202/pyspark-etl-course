import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from shared.spark_utils import create_spark_session


def main() -> None:
    spark = create_spark_session("chapter-12-jdbc-read-write-database-exercise")

    try:
        data = [
            ("001", "Nguyen Van A", "Ha Noi", 100000.0),
            ("002", "Tran Thi B", "Ho Chi Minh", 250000.0),
            ("005", "Hoang Thi F", "Ho Chi Minh", 500000.0),
        ]
        columns = ["customer_id", "customer_name", "province_name", "total_amount"]

        customer_360_df = spark.createDataFrame(data, columns)

        print("=== CUSTOMER 360 SAMPLE DATA ===")
        customer_360_df.show(truncate=False)

        print("=== JDBC WRITE EXAMPLE - NOT EXECUTED ===")
        print("""
(
    customer_360_df.write
    .format("jdbc")
    .mode("append")
    .option("url", "jdbc:postgresql://localhost:5432/vss")
    .option("dbtable", "vss_360.customer_360")
    .option("user", "your_user")
    .option("password", "your_password")
    .option("driver", "org.postgresql.Driver")
    .save()
)
""".strip())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
