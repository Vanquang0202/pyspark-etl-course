# Chapter 13 - Window Functions

## Bài này làm gì?

Bài này demo window function trong PySpark.
Window giúp tính toán trong từng nhóm dữ liệu nhưng vẫn giữ được từng dòng.
Ví dụ hay gặp là lấy bản ghi mới nhất của mỗi customer.
Cũng dùng được để so sánh giao dịch hiện tại với giao dịch trước hoặc sau.

## Học được gì?

- Dùng `Window.partitionBy()` để chia nhóm.
- Dùng `orderBy()` để sắp xếp trong từng nhóm.
- Học `row_number`, `rank`, `dense_rank`.
- Học `lag`, `lead` để nhìn dòng trước/sau.
- Học `sum().over()` để tính tổng theo nhóm.

## Khái niệm chính

`Window`: vùng dữ liệu để tính toán theo nhóm, ví dụ theo `customer_id`.

`partitionBy`: chia dữ liệu thành từng nhóm riêng trong window.

`orderBy`: sắp xếp record trong mỗi nhóm, hay dùng với ngày giao dịch.

`row_number`: đánh số từng dòng, thường dùng để lấy top 1.

`rank` / `dense_rank`: xếp hạng trong nhóm, khác nhau ở cách xử lý đồng hạng.

`lag` / `lead`: lấy giá trị dòng trước hoặc dòng sau để so sánh.

## Lệnh chạy

```bash
python chapters/13_window_functions/demo.py
python chapters/13_window_functions/exercise.py
```

## Nhớ nhanh

- Muốn lấy record mới nhất: sort ngày giảm dần rồi `row_number = 1`.
- Window khác `groupBy` vì không gom mất dòng chi tiết.
- `lag` xem dòng trước, `lead` xem dòng sau.
- Rất hợp cho bài toán trạng thái mới nhất của customer.
