# Chapter 03 - Complex Data Types

## Muc tieu

Chuong nay bam theo phan Complex Data Types trong PySpark Tour of Types:

- `ArrayType`: mot cot chua nhieu gia tri cung kieu.
- Nested `StructType`: mot cot chua mot record co cau truc rieng.
- `MapType`: mot cot chua cac cap key-value.

Vi du chinh la customer co nhieu dia chi va mot tap preferences linh dong.

## Cau truc vi du

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

Complex types phu hop voi JSON va du lieu long nhau. Tuy nhien, khong nen dua moi thu vao mot cot phuc tap neu cac cot phang de query va bao tri hon.

## Chay

```powershell
python chapters/03_complex_data_types/demo.py
python chapters/03_complex_data_types/exercise.py
```

## Bai tap

File `exercise.py` tao employee co danh sach skills, contact dang struct va metadata dang map. Hay them mot employee va truy cap tung field long nhau bang `select()`.

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
