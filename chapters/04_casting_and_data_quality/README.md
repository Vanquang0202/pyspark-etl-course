# Chapter 04 - Casting and Data Quality

## Mục tiêu

Sau chương này, bạn có thể:

- Ép kiểu dữ liệu bằng `cast()` và parse ngày bằng `to_date()`.
- Phát hiện dữ liệu bẩn thông qua các giá trị null xuất hiện sau khi cast.
- Tạo cờ kiểm tra chất lượng cho từng field.
- Tạo trạng thái chất lượng chung cho mỗi record.
- Tách dữ liệu thành `valid_df` và `invalid_df` trước bước ETL tiếp theo.

## Vì sao dữ liệu nguồn thường là string?

CSV chỉ lưu nội dung dạng text và không có schema chặt chẽ. API cũng thường trả dữ liệu qua JSON, trong đó số hoặc ngày tháng có thể được biểu diễn bằng chuỗi. Ví dụ, `"100000.50"` nhìn giống số tiền và `"01/01/1995"` nhìn giống ngày, nhưng Spark không nên tự giả định kiểu dữ liệu nếu hợp đồng nguồn chưa rõ ràng.

Đọc dữ liệu nguồn dưới dạng string trước giúp ta giữ nguyên giá trị gốc để kiểm tra. Sau đó ETL mới cast sang kiểu đích rõ ràng:

- ID dạng số có thể cast sang integer để kiểm tra tính hợp lệ.
- Mã tỉnh nên giữ là string vì đây là mã phân loại và có thể có số `0` ở đầu, ví dụ `"01"`.
- Số tiền nên dùng `DecimalType` để giữ precision và scale cố định.
- Ngày tháng phải được parse bằng format đã thống nhất.

## Vì sao phải cast trước khi load vào DB?

Bảng đích thường có schema cụ thể như `INTEGER`, `DECIMAL` và `DATE`. Nếu đưa thẳng string bẩn vào DB, job có thể fail giữa chừng hoặc dữ liệu được lưu sai kiểu. Cast và validate trước giúp phát hiện lỗi sớm, bảo vệ bảng đích và làm kết quả ETL dễ kiểm soát hơn.

## Cast lỗi, null và ANSI mode

Giá trị như `"abc"` không thể cast thành integer/decimal. Khi `spark.sql.ansi.enabled=false`, Spark thường trả về null. Khi ANSI mode bật, Spark có thể ném exception và dừng action. Demo và exercise tạm tắt ANSI mode để quan sát null sau cast, sau đó khôi phục cấu hình ban đầu.

Trong production, cần chọn rõ một chiến lược:

- Bật ANSI để fail fast.
- Validate dữ liệu trước khi dùng `cast()`.
- Dùng các hàm `try_cast`/`try_to_timestamp` khi muốn lỗi chuyển thành null có kiểm soát.

## Tách valid và invalid

Không nên để record lỗi đi tiếp vào bước join, aggregate hoặc load DB. `valid_df` chứa dữ liệu đạt yêu cầu để ETL tiếp tục. `invalid_df` giữ dữ liệu lỗi cùng `data_quality_status` để điều tra.

Trong hệ thống thực tế, `invalid_df` có thể được:

- Ghi vào reject table.
- Ghi vào error log hoặc error data lake zone.
- Gửi cảnh báo cho đội quản lý dữ liệu.
- Sửa và chạy lại sau khi nguồn được điều chỉnh.

## Chạy trên Ubuntu/WSL

```bash
cd /mnt/e/PySpark/pyspark-etl-course
source .venv-linux/bin/activate
python chapters/04_casting_and_data_quality/demo.py
python chapters/04_casting_and_data_quality/exercise.py
```

## Output kỳ vọng

Demo hiển thị schema nguồn gồm các string, sau đó schema mới có:

```text
customer_id_int: integer
province_code_str: string
amount_decimal: decimal(18,2)
birth_date_parsed: date
is_valid_customer_id/is_valid_amount/is_valid_birth_date: boolean
data_quality_status: string
```

Exercise có cả record tốt và record lỗi. Kết quả tổng hợp kỳ vọng:

```text
total_records: 6
valid_records: 1
invalid_records: 5
```

Các record có amount `"abc"`, amount âm, ngày sai format, thiếu customer ID hoặc thiếu province code phải nằm trong `invalid_df`.

## Quick Notes

### Bài này học gì?

* Ép kiểu dữ liệu nguồn từ string sang kiểu đích.
* Phát hiện dữ liệu lỗi sau khi cast hoặc parse ngày.
* Tách record hợp lệ và không hợp lệ trước bước ETL tiếp theo.

### Khái niệm chính

* `cast()`: đổi kiểu dữ liệu của một cột.
* `to_date()`: parse chuỗi thành ngày theo format.
* `DecimalType`: kiểu số phù hợp cho tiền và số cần precision.
* `when()/otherwise()`: tạo rule kiểm tra và trạng thái dữ liệu.
* `valid_df`: dữ liệu đủ điều kiện đi tiếp.
* `invalid_df`: dữ liệu lỗi cần điều tra hoặc reject.

### Nhớ nhanh

* Cast lỗi thường tạo null khi ANSI mode tắt.
* Không nên để record lỗi đi tiếp vào join, aggregate hoặc load.
