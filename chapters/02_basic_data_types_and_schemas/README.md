# Chapter 02 - Basic Data Types and Schemas

## Muc tieu

Chuong nay bam theo phan Basic Data Types trong PySpark Tour of Types:

- Nhan biet cac kieu so, chuoi, boolean, date va timestamp.
- Khai bao schema ro rang bang `StructType` va `StructField`.
- Hieu `nullable=True` va `nullable=False`.
- Phan biet `FloatType`, `DoubleType` va `DecimalType`.
- Dung `StringType` cho ma dinh danh de giu so 0 o dau.
- Dung `DecimalType` cho so tien can do chinh xac co dinh.

## Kieu du lieu quan trong

| PySpark type | Python type | Vi du |
|---|---|---|
| `IntegerType`, `LongType` | `int` | tuoi, so luong |
| `FloatType`, `DoubleType` | `float` | ty le, diem do |
| `DecimalType` | `decimal.Decimal` | so tien |
| `StringType` | `str` | ID, ten, ma tinh |
| `BooleanType` | `bool` | trang thai |
| `DateType` | `datetime.date` | ngay sinh |
| `TimestampType` | `datetime.datetime` | thoi diem giao dich |

`FloatType` co do chinh xac 32-bit, `DoubleType` la 64-bit. Hai kieu nay co the co sai so nhi phan. `DecimalType(precision, scale)` phu hop hon cho tien vi precision va scale duoc khai bao ro rang.

## Explicit schema

`StructType` mo ta ca bang. Moi `StructField` mo ta ten cot, kieu du lieu va cot co duoc phep null hay khong.

- `nullable=False`: cot bat buoc co gia tri.
- `nullable=True`: cot co the chua `null`.

Explicit schema giup hop dong du lieu ro rang va tranh suy luan sai kieu khi doc du lieu.

## Chay

```powershell
python chapters/02_basic_data_types_and_schemas/demo.py
python chapters/02_basic_data_types_and_schemas/exercise.py
```

## Bai tap

File `exercise.py` tao bang san pham voi schema ro rang. Hay thu them cot ngay tao hoac cot so luong, chon kieu du lieu va `nullable` phu hop, sau do dung `printSchema()` de kiem tra.

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
