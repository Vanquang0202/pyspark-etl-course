from demo import jdbc_options


if __name__ == "__main__":
    # Bài tập: thay customer_source bằng query JDBC và chọn mode append/overwrite.
    print(jdbc_options("public.customer_source"))
