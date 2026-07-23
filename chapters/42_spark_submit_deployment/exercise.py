from demo import parse_args


if __name__ == "__main__":
    args = parse_args()
    # Bài tập: thêm argument --input-path và truyền vào spark-submit.
    print(f"Exercise deployment plan for {args.run_date} in {args.environment}")
