from app import load_customers


def find_customer(customer_id: str) -> dict[str, str] | None:
    """Bài tập: bổ sung validate customer_id hoặc filter theo province."""
    return next((item for item in load_customers() if item["customer_id"] == customer_id), None)


if __name__ == "__main__":
    print(find_customer("001"))
