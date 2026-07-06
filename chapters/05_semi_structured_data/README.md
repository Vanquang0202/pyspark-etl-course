# Chapter 05 - Semi-Structured Data

## Mục tiêu

Chương này bám theo phần Semi-Structured Data Processing trong PySpark Tour of Types:

- Đọc file JSON bằng `spark.read.json()`.
- Parse chuỗi JSON bằng `from_json()` và explicit schema.
- Chuyển struct/map thành chuỗi JSON bằng `to_json()`.
- Đọc XML bằng data source `xml` có sẵn từ Spark 4.0.
- Làm quen với `VARIANT`, `try_parse_json()` và `try_variant_get()` trong Spark 4.

## Dữ liệu mẫu

- `data/input/customer_profiles.json`: JSON Lines, mỗi dòng là một object.
- `data/input/customer_profiles.xml`: root `customers`, mỗi record có tag `customer`.

## Giới hạn và lưu ý

- XML data source trong demo này yêu cầu Spark 4.0 trở lên. Các bản Spark cũ có thể cần package `spark-xml` bên ngoài.
- VARIANT là tính năng Spark 4.0. Demo sử dụng `try_parse_json` để giá trị JSON không hợp lệ trở thành null thay vì làm dừng job.
- Không phải hệ thống đích nào cũng hỗ trợ VARIANT. Cần kiểm tra compatibility trước khi dùng trong pipeline thực tế.
- Trên Windows local, các phần này chạy bằng JVM/Python của project. Nếu XML hoặc VARIANT lỗi, hãy kiểm tra lại `pyspark==4.0.1` trước khi thêm package bên ngoài.

## Chạy

```powershell
python chapters/05_semi_structured_data/demo.py
python chapters/05_semi_structured_data/exercise.py
```

## Bài tập

`exercise.py` parse một cột JSON chứa thông tin order. Hãy thêm một record lỗi và quan sát `from_json()` trả về null như thế nào.

## Quick Notes

### Bài này học gì?

* Đọc và parse dữ liệu semi-structured như JSON/XML.
* Dùng schema rõ ràng khi parse chuỗi JSON trong cột.
* Chuyển đổi giữa struct/map và chuỗi JSON khi cần.

### Khái niệm chính

* `spark.read.json()`: đọc file JSON thành DataFrame.
* `from_json()`: parse chuỗi JSON thành struct/map theo schema.
* `to_json()`: đổi struct/map về chuỗi JSON.
* XML data source: đọc XML bằng Spark data source phù hợp.
* `VARIANT`: kiểu linh hoạt cho dữ liệu JSON trong Spark 4.
* `try_parse_json()`: parse JSON lỗi thành null có kiểm soát.

### Nhớ nhanh

* JSON/XML nên đi kèm explicit schema để pipeline ổn định.
* Semi-structured data linh hoạt nhưng cần validate kỹ hơn dữ liệu phẳng.
