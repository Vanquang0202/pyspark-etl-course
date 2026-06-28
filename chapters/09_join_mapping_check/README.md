# Chapter 09 - Join Mapping Check

## Mục tiêu học

Sau chương này, bạn có thể:

- Join hai DataFrame bằng một key chung.
- Dùng bảng danh mục để enrich dữ liệu nguồn.
- Phân biệt `inner join` và `left join`.
- Tạo cột trạng thái mapping.
- Tách dữ liệu mapping được và không mapping được.

## Kiến thức chính

`inner join` chỉ giữ các dòng có key tồn tại ở cả hai DataFrame. Nếu customer có `province_code` không tồn tại trong `province.csv`, dòng đó sẽ bị loại khỏi kết quả.

`left join` giữ toàn bộ dữ liệu bên trái, sau đó thêm thông tin từ bảng bên phải nếu mapping được. Khi không tìm thấy danh mục, các cột từ bảng bên phải sẽ là null.

Trong chapter này:

- `customer.csv` là dữ liệu giao dịch hoặc dữ liệu nguồn.
- `province.csv` là bảng master data.
- `province_code` là key dùng để join.
- `mapping_status` cho biết record đã mapping được hay chưa.

## Liên hệ ETL thực tế

ETL mapping danh mục thường dùng `left join` vì không nên làm mất record nguồn khi master data bị thiếu hoặc code nguồn bị sai. Thay vào đó, job nên giữ record và đánh dấu lỗi mapping.

Các record trong `unmapped_df` thường được đưa vào:

- Reject table.
- Error report.
- Data quality dashboard.
- Ticket để team nguồn hoặc team master data xử lý.

## Cách chạy trên Ubuntu/WSL

```bash
cd /mnt/e/PySpark/pyspark-etl-course
source .venv-linux/bin/activate

python chapters/09_join_mapping_check/demo.py
python chapters/09_join_mapping_check/exercise.py
```

## Output kỳ vọng

Demo hiển thị schema của customer và province, kết quả `inner join`, kết quả `left join` có `mapping_status`, sau đó in:

```text
total_customers
mapped_customers
unmapped_customers
```

Exercise show riêng:

```text
mapped_df
unmapped_df
```

Output columns chính:

```text
customer_id, name, province_code, province_name, mapping_status
```

## Quick Notes

### Bài này học gì?

* Join dữ liệu nguồn với bảng mapping/master data.
* Phân biệt record mapping được và không mapping được.
* Giữ lại unmapped records để kiểm tra chất lượng dữ liệu.

### Khái niệm chính

* `join()`: nối hai DataFrame theo key chung.
* `inner join`: chỉ giữ record có key ở cả hai bên.
* `left join`: giữ toàn bộ dữ liệu bên trái.
* Mapping table: bảng danh mục dùng để enrich dữ liệu.
* `mapping_status`: trạng thái cho biết record có map được không.
* `unmapped_df`: tập record không tìm thấy trong bảng mapping.

### Nhớ nhanh

* ETL mapping thường dùng left join để không làm mất dữ liệu nguồn.
* Unmapped records là tín hiệu cần kiểm tra source hoặc master data.
