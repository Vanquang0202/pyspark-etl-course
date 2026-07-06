# Chapter 03 - Complex Data Types

## Mục tiêu

Chương này bám theo phần Complex Data Types trong PySpark Tour of Types:

- `ArrayType`: một cột chứa nhiều giá trị cùng kiểu.
- Nested `StructType`: một cột chứa một record có cấu trúc riêng.
- `MapType`: một cột chứa các cặp key-value.

Ví dụ chính là customer có nhiều địa chỉ và một tập preferences linh động.

## Cấu trúc ví dụ

```text
customer_id: string
name: string
addresses: array
  element: struct
    street: string
    city: string
    zip_code: string
preferences: map<string, string>
```

Complex types phù hợp với JSON và dữ liệu lồng nhau. Tuy nhiên, không nên đưa mọi thứ vào một cột phức tạp nếu các cột phẳng dễ query và bảo trì hơn.

## Chạy

```powershell
python chapters/03_complex_data_types/demo.py
python chapters/03_complex_data_types/exercise.py
```

## Bài tập

File `exercise.py` tạo employee có danh sách skills, contact dạng struct và metadata dạng map. Hãy thêm một employee và truy cập từng field lồng nhau bằng `select()`.

## Quick Notes

### Bài này học gì?

* Làm việc với dữ liệu lồng nhau trong DataFrame.
* Biết dùng array, struct và map cho dữ liệu phức tạp.
* Truy cập field con trong nested data bằng `select()`.

### Khái niệm chính

* `ArrayType`: một cột chứa danh sách nhiều giá trị cùng kiểu.
* `StructType`: một cột chứa record con có nhiều field.
* `MapType`: một cột chứa các cặp key-value.
* Nested field: field nằm bên trong struct hoặc array of struct.
* `select()`: chọn cột hoặc field con cần xem/xử lý.

### Nhớ nhanh

* Complex types rất hợp với JSON và dữ liệu lồng nhau.
* Chỉ dùng nested data khi nó giúp mô hình dữ liệu rõ hơn.
