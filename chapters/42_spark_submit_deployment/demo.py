import argparse


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark submit deployment demo.")
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--environment", default="local")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    print(f"Spark job plan: run_date={args.run_date}, environment={args.environment}")
