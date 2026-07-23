from app import load_customers, province_summary


if __name__ == "__main__":
    customers = load_customers()
    print("GET /customers ->", customers[:2])
    print("GET /reports/province-summary ->", province_summary(customers))
