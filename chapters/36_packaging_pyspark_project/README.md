# Chapter 36 - Packaging PySpark Project

## Bài này làm gì?

Bài này demo cách tách logic PySpark thành module nhỏ.
`main.py` chỉ điều phối job, còn transform nằm trong package riêng.

## Học được gì?

- Tổ chức project PySpark thành nhiều file.
- Tách hàm transform vào module riêng.
- Import lại module trong `main.py`.
- Viết code dễ test và dễ tái sử dụng hơn.

## Vì sao không nên để toàn bộ logic trong một file?

Một file lớn làm job khó đọc, khó test và khó sửa.
Khi transform, validate, read/write và config nằm chung một chỗ, thay đổi nhỏ cũng dễ ảnh hưởng phần khác.

Package/module giúp:

- Tái sử dụng hàm transform ở nhiều job.
- Unit test logic nhỏ dễ hơn.
- `main.py` ngắn và rõ flow xử lý.
- Dễ liên hệ với `final_project`, nơi ETL job có nhiều bước hơn demo đơn giản.

## Cấu trúc demo

```text
chapters/36_packaging_pyspark_project/
|-- etl_package/
|   |-- __init__.py
|   `-- transforms.py
|-- main.py
|-- demo.py
|-- exercise.py
`-- README.md
```

## Lệnh chạy

```bash
python chapters/36_packaging_pyspark_project/demo.py
python chapters/36_packaging_pyspark_project/main.py
python chapters/36_packaging_pyspark_project/exercise.py
```

## Nhớ nhanh

- `main.py` nên mô tả flow chính.
- Transform nên là hàm riêng để dễ test.
- Final project có thể phát triển tiếp theo hướng package hóa như chapter này.
