# Chapter 19 - Handling Larger Data

## Bài này làm gì?

Bài này demo cách nghĩ khi dữ liệu bắt đầu lớn hơn.
Ví dụ dùng `range` để tạo dữ liệu giả lập, không cần file input lớn.
Trọng tâm là giảm dữ liệu càng sớm càng tốt trước khi join hoặc groupBy.

## Học được gì?

- Tạo DataFrame nhiều dòng bằng `spark.range`.
- Kiểm soát số partition bằng `repartition`.
- Filter trước khi groupBy hoặc join.
- Chỉ `select` các cột cần dùng.
- Tránh `collect()` với dữ liệu lớn.

## Khái niệm chính

`Partition`: phần dữ liệu nhỏ Spark dùng để xử lý song song.

`Repartition`: chia lại dữ liệu thành số partition mới, có thể tạo shuffle.

`Filter sớm`: loại dòng không cần thiết trước bước nặng.

`Select cột cần thiết`: giảm kích thước dữ liệu phải xử lý.

## Lệnh chạy

```bash
python chapters/19_handling_larger_data/demo.py
python chapters/19_handling_larger_data/exercise.py
```

## Nhớ nhanh

- Dữ liệu lớn cần giảm cột và giảm dòng sớm.
- Không `collect()` dữ liệu lớn về driver.
- Nên kiểm soát partition, nhưng không repartition bừa.
- `groupBy` và `join` thường là bước cần chú ý performance.
