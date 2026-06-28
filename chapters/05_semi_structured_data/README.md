# Chapter 05 - Semi-Structured Data

## Muc tieu

Chuong nay bam theo phan Semi-Structured Data Processing trong PySpark Tour of Types:

- Doc file JSON bang `spark.read.json()`.
- Parse chuoi JSON bang `from_json()` va explicit schema.
- Chuyen struct/map thanh chuoi JSON bang `to_json()`.
- Doc XML bang data source `xml` co san tu Spark 4.0.
- Lam quen voi `VARIANT`, `try_parse_json()` va `try_variant_get()` trong Spark 4.

## Du lieu mau

- `data/input/customer_profiles.json`: JSON Lines, moi dong la mot object.
- `data/input/customer_profiles.xml`: root `customers`, moi record co tag `customer`.

## Gioi han va luu y

- XML data source trong demo nay yeu cau Spark 4.0 tro len. Cac ban Spark cu co the can package `spark-xml` ben ngoai.
- VARIANT la tinh nang Spark 4.0. Demo su dung `try_parse_json` de gia tri JSON khong hop le tro thanh null thay vi lam dung job.
- Khong phai he thong dich nao cung ho tro VARIANT. Can kiem tra compatibility truoc khi dung trong pipeline thuc te.
- Tren Windows local, cac phan nay chay bang JVM/Python cua project. Neu XML hoac VARIANT loi, hay kiem tra lai `pyspark==4.0.1` truoc khi them package ben ngoai.

## Chay

```powershell
python chapters/05_semi_structured_data/demo.py
python chapters/05_semi_structured_data/exercise.py
```

## Bai tap

`exercise.py` parse mot cot JSON chua thong tin order. Hay them mot record loi va quan sat `from_json()` tra ve null nhu the nao.

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
