import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from shared.spark_utils import create_spark_session


def main():
    # 1. Tạo SparkSession
    spark = create_spark_session("chapter-01-dataframe-basics-exercise")

    # 2. Tạo dữ liệu mẫu dạng list of dict
    customers = [
        {
            "customer_id": "001",
            "name": "Nguyen Van A",
            "age": 30,
            "gender": "M",
            "province_code": "01"
        },
        {
            "customer_id": "002",
            "name": "Tran Thi B",
            "age": 25,
            "gender": "F",
            "province_code": "79"
        },
        {
            "customer_id": "003",
            "name": "Le Van C",
            "age": 35,
            "gender": "M",
            "province_code": "48"
        },
        {
            "customer_id": "004",
            "name": "Pham Thi D",
            "age": 28,
            "gender": "F",
            "province_code": "01"
        }
    ]

    # 3. Tạo DataFrame từ dữ liệu mẫu
    customer_df = spark.createDataFrame(customers)

    print("===== 1. HIEN THI DU LIEU DATAFRAME =====")
    customer_df.show()

    print("===== 2. HIEN THI SCHEMA =====")
    customer_df.printSchema()

    print("===== 3. DEM SO DONG =====")
    total_rows = customer_df.count()
    print(f"Total rows: {total_rows}")

    print("===== 4. SELECT MOT SO COT =====")
    selected_df = customer_df.select(
        "customer_id",
        "name",
        "age"
    )
    selected_df.show()

    print("===== 5. FILTER CUSTOMER CO AGE > 26 =====")
    filtered_df = customer_df.filter(customer_df["age"] > 26)
    filtered_df.show()

    print("===== 6. DOI TEN COT name THANH full_name =====")
    renamed_df = customer_df.withColumnRenamed("name", "full_name")
    renamed_df.show()

    print("===== 7. TAO TEMP VIEW VA QUERY BANG SQL =====")
    customer_df.createOrReplaceTempView("customers")

    sql_df = spark.sql("""
        SELECT customer_id, name, age, province_code
        FROM customers
        WHERE age > 26
    """)

    sql_df.show()

    # 8. Dừng SparkSession
    spark.stop()


if __name__ == "__main__":
    main()
