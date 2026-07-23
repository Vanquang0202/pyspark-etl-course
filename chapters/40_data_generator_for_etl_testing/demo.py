from generator import build_event


if __name__ == "__main__":
    print(build_event(1, invalid_rate=0.0))
    print(build_event(2, invalid_rate=1.0))
