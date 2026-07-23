# Chapter 32 - Environment Config Dev Test Prod

## Bài này làm gì?

Bài này demo cách tách config theo môi trường `dev`, `test` và `prod`.
Mỗi môi trường có một file JSON riêng.

## Học được gì?

- Tạo config mẫu cho từng môi trường.
- Đọc config theo biến môi trường hoặc argument.
- Dùng chung một logic ETL cho nhiều môi trường.
- In rõ config đang được dùng khi job chạy.

## Cách đổi môi trường khi chạy ETL

Có thể truyền môi trường bằng argument:

```bash
python chapters/32_environment_config_dev_test_prod/demo.py --env dev
python chapters/32_environment_config_dev_test_prod/demo.py --env test
python chapters/32_environment_config_dev_test_prod/demo.py --env prod
```

Hoặc dùng biến môi trường:

```bash
ETL_ENV=test python chapters/32_environment_config_dev_test_prod/demo.py
```

Argument `--env` được ưu tiên hơn biến môi trường `ETL_ENV`.

## Nhớ nhanh

- Dev/test/prod nên khác config, không khác code.
- Config prod trong bài chỉ là ví dụ, không chứa secret thật.
- Không commit password, token hoặc connection string thật lên Git.
