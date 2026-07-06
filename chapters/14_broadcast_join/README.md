# Chapter 14 - Broadcast Join

## Bài này làm gì?

Bài này demo broadcast join trong PySpark.
Ý tưởng là join bảng dữ liệu lớn với một bảng danh mục nhỏ.
Spark gửi bảng nhỏ đến các executor để join nhanh hơn và giảm shuffle.
Ví dụ trong bài là customer join với province.

## Học được gì?

- Khi nào nên dùng broadcast join.
- Cách dùng `broadcast()` trong PySpark.
- Vì sao bảng danh mục nhỏ hợp để broadcast.
- Không nên broadcast bảng lớn.
- Có thể xem plan bằng `explain()`.

## Khái niệm chính

`Broadcast join`: Spark copy bảng nhỏ tới các executor để join với bảng lớn.

`Bảng lớn`: bảng dữ liệu chính, ví dụ customer hoặc transaction.

`Bảng nhỏ`: bảng danh mục/master data, ví dụ province, status, product type.

`Shuffle`: việc Spark phải di chuyển dữ liệu giữa partition/node, thường tốn chi phí.

`BroadcastHashJoin`: kiểu join có thể thấy trong physical plan khi Spark dùng broadcast.

## Lệnh chạy

```bash
python chapters/14_broadcast_join/demo.py
python chapters/14_broadcast_join/exercise.py
```

## Nhớ nhanh

- Broadcast bảng nhỏ, không broadcast bảng lớn.
- Province master là ví dụ rất hợp để broadcast.
- Broadcast giúp giảm shuffle khi join.
- Bảng nhỏ vẫn phải đủ nhỏ để vừa memory.
