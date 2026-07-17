# Chapter 24 - PySpark Unit Testing

## Bài này làm gì?

Bài này demo cách test logic ETL nhỏ bằng `pytest`.
Thay vì chỉ nhìn kết quả bằng `show()`, ta viết test để kiểm tra hàm clean, cast và validate có chạy đúng không.

## Học được gì?

- Tạo `SparkSession` dùng riêng cho test.
- Tách logic ETL thành hàm nhỏ để dễ test.
- Test clean dữ liệu như `trim()` và chuẩn hóa tên.
- Test cast amount từ string sang double.
- Test rule validate record hợp lệ và không hợp lệ.

## Vì sao ETL cần unit test?

ETL thường có nhiều rule nghiệp vụ nhỏ.
Nếu sửa một rule mà không có test, rất dễ làm sai dữ liệu ở bước sau.

Unit test giúp:

- Phát hiện lỗi sớm trước khi chạy job lớn.
- Kiểm tra lại logic khi refactor code.
- Ghi rõ kỳ vọng của rule bằng code.
- Giảm việc kiểm tra thủ công bằng mắt.

## Lệnh chạy

```bash
pytest chapters/24_pyspark_unit_testing/demo.py
pytest chapters/24_pyspark_unit_testing/exercise.py
```

## Nhớ nhanh

- Nên tách transform thành hàm nhận DataFrame và trả DataFrame.
- Test nên dùng dữ liệu nhỏ, dễ đọc.
- Không cần test Spark internals, chỉ test rule ETL của mình.
