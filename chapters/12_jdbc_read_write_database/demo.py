JDBC_CONFIGS = {
    "oracle": {
        "url": "jdbc:oracle:thin:@//localhost:1521/XEPDB1",
        "driver": "oracle.jdbc.OracleDriver",
        "dbtable": "VSS_ODS.CUSTOMER_RAW",
    },
    "postgresql": {
        "url": "jdbc:postgresql://localhost:5432/vss",
        "driver": "org.postgresql.Driver",
        "dbtable": "public.customer_raw",
    },
    "sqlite": {
        "url": "jdbc:sqlite:data/input/sample.db",
        "driver": "org.sqlite.JDBC",
        "dbtable": "customer_raw",
    },
}


def main() -> None:
    print("=== JDBC CONFIG EXAMPLES ===")
    for database_name, config in JDBC_CONFIGS.items():
        print(f"\n[{database_name}]")
        for key, value in config.items():
            print(f"{key}: {value}")

    print("\n=== SAFE READ EXAMPLE - NOT EXECUTED ===")
    print("""
df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "VSS_ODS.CUSTOMER_RAW")
    .option("user", username)
    .option("password", password)
    .option("driver", driver_class)
    .load()
)
""".strip())

    print("\n=== SAFE WRITE EXAMPLE - NOT EXECUTED ===")
    print("""
(
    customer_360_df.write
    .format("jdbc")
    .mode("append")
    .option("url", jdbc_url)
    .option("dbtable", "VSS_360.CUSTOMER_360")
    .option("user", username)
    .option("password", password)
    .option("driver", driver_class)
    .save()
)
""".strip())

    print("\n=== LARGE TABLE READ OPTIONS ===")
    print("""
.option("partitionColumn", "customer_id_num")
.option("lowerBound", 1)
.option("upperBound", 1000000)
.option("numPartitions", 8)
""".strip())


if __name__ == "__main__":
    main()
