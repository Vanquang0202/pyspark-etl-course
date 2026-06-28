import os
import sys
from pathlib import Path

# Add project root to sys.path để import được shared khi chạy từ root project
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from shared.spark_utils import create_spark_session


def main():
    spark = create_spark_session("chapter-07-read-write-files-exercise")

    try:
        # ============================================================
        # 1. Khai báo đường dẫn input/output
        # ============================================================

        customer_input_path = PROJECT_ROOT / "data" / "input" / "customer.csv"
        province_input_path = PROJECT_ROOT / "data" / "input" / "province.csv"

        output_root = PROJECT_ROOT / "data" / "output" / "chapter_07"
        customer_parquet_output = output_root / "customer_parquet"
        customer_csv_output = output_root / "customer_csv"
        province_parquet_output = output_root / "province_parquet"

        print("===== INPUT PATHS =====")
        print(f"Customer input: {customer_input_path}")
        print(f"Province input: {province_input_path}")

        # ============================================================
        # 2. Đọc file customer.csv
        # ============================================================

        customer_df = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(str(customer_input_path))

        print("===== 1. CUSTOMER DATA =====")
        customer_df.show(truncate=False)

        print("===== 2. CUSTOMER SCHEMA =====")
        customer_df.printSchema()

        print("===== 3. CUSTOMER COUNT =====")
        print(f"Total customers: {customer_df.count()}")

        # ============================================================
        # 3. Đọc file province.csv
        # ============================================================

        province_df = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(str(province_input_path))

        print("===== 4. PROVINCE DATA =====")
        province_df.show(truncate=False)

        print("===== 5. PROVINCE SCHEMA =====")
        province_df.printSchema()

        print("===== 6. PROVINCE COUNT =====")
        print(f"Total provinces: {province_df.count()}")

        # ============================================================
        # 4. Ghi customer ra Parquet
        # ============================================================

        customer_df.write \
            .mode("overwrite") \
            .parquet(str(customer_parquet_output))

        print(f"Written customer parquet to: {customer_parquet_output}")

        # ============================================================
        # 5. Ghi customer ra CSV
        # ============================================================

        customer_df.write \
            .mode("overwrite") \
            .option("header", True) \
            .csv(str(customer_csv_output))

        print(f"Written customer csv to: {customer_csv_output}")

        # ============================================================
        # 6. Ghi province ra Parquet
        # ============================================================

        province_df.write \
            .mode("overwrite") \
            .parquet(str(province_parquet_output))

        print(f"Written province parquet to: {province_parquet_output}")

        # ============================================================
        # 7. Đọc lại file Parquet vừa ghi để kiểm tra
        # ============================================================

        customer_parquet_df = spark.read.parquet(str(customer_parquet_output))

        print("===== 7. READ BACK CUSTOMER PARQUET =====")
        customer_parquet_df.show(truncate=False)
        customer_parquet_df.printSchema()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
