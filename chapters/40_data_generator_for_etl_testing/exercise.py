from generator import build_event


if __name__ == "__main__":
    # Bài tập: tạo thêm invalid record thiếu province_code.
    print(build_event(1, invalid_rate=0.2))
