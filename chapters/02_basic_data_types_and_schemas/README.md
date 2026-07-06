# Chapter 02 - Basic Data Types and Schemas

## Mục tiêu

Chương này bám theo phần Basic Data Types trong PySpark Tour of Types:

- Nhận biết các kiểu số, chuỗi, boolean, date và timestamp.
- Khai báo schema rõ ràng bằng `StructType` và `StructField`.
- Hiểu `nullable=True` và `nullable=False`.
- Phân biệt `FloatType`, `DoubleType` và `DecimalType`.
- Dùng `StringType` cho mã định danh để giữ số 0 ở đầu.
- Dùng `DecimalType` cho số tiền cần độ chính xác cố định.

## Kiểu dữ liệu quan trọng

| PySpark type | Python type | Ví dụ |
|---|---|---|
| `IntegerType`, `LongType` | `int` | tuổi, số lượng |
| `FloatType`, `DoubleType` | `float` | tỷ lệ, điểm đo |
| `DecimalType` | `decimal.Decimal` | số tiền |
| `StringType` | `str` | ID, tên, mã tỉnh |
| `BooleanType` | `bool` | trạng thái |
| `DateType` | `datetime.date` | ngày sinh |
| `TimestampType` | `datetime.datetime` | thời điểm giao dịch |

`FloatType` có độ chính xác 32-bit, `DoubleType` là 64-bit. Hai kiểu này có thể có sai số nhị phân. `DecimalType(precision, scale)` phù hợp hơn cho tiền vì precision và scale được khai báo rõ ràng.

## Explicit schema

`StructType` mô tả cả bảng. Mỗi `StructField` mô tả tên cột, kiểu dữ liệu và cột có được phép null hay không.

- `nullable=False`: cột bắt buộc có giá trị.
- `nullable=True`: cột có thể chứa `null`.

Explicit schema giúp hợp đồng dữ liệu rõ ràng và tránh suy luận sai kiểu khi đọc dữ liệu.

## Chạy

```powershell
python chapters/02_basic_data_types_and_schemas/demo.py
python chapters/02_basic_data_types_and_schemas/exercise.py
```

## Bài tập

File `exercise.py` tạo bảng sản phẩm với schema rõ ràng. Hãy thử thêm cột ngày tạo hoặc cột số lượng, chọn kiểu dữ liệu và `nullable` phù hợp, sau đó dùng `printSchema()` để kiểm tra.

## Quick Notes

### Bài này học gì?

* Khai báo schema rõ ràng thay vì để Spark tự đoán kiểu.
* Chọn đúng data type cho số, chuỗi, ngày tháng và tiền.
* Hiểu ý nghĩa của `nullable` trong hợp đồng dữ liệu.

### Khái niệm chính

* `StructType`: mô tả schema tổng thể của DataFrame.
* `StructField`: mô tả từng cột, kiểu dữ liệu và nullable.
* `StringType`: phù hợp cho text, mã định danh và mã có số 0 đầu.
* `DecimalType`: dùng cho số tiền cần độ chính xác cố định.
* `DateType`: biểu diễn ngày không kèm giờ.
* `TimestampType`: biểu diễn thời điểm có cả ngày và giờ.

### Nhớ nhanh

* Explicit schema giúp dữ liệu ổn định và dễ kiểm soát hơn.
* ID/code thường nên giữ dạng string, không phải số.
