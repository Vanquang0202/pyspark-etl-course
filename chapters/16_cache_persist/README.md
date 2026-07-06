# Chapter 16 - Cache and Persist

## Bài này làm gì?

Bài này demo `cache()` và `persist()` trong PySpark.
Ý chính là giữ lại DataFrame khi nó được dùng lại nhiều lần.
Vì Spark lazy execution, nếu không cache thì cùng một DataFrame có thể bị tính lại.
Cache có thể giúp nhanh hơn, nhưng không phải lúc nào cũng nên dùng.

## Học được gì?

- Spark chạy theo lazy execution.
- Transformation chưa chạy ngay.
- Action mới kích hoạt Spark job.
- `cache()` giúp tái sử dụng DataFrame.
- `persist()` cho phép chọn cách lưu chi tiết hơn.
- Không nên cache bừa vì tốn RAM.

## Khái niệm chính

`Lazy execution`: Spark chỉ tạo plan trước, chưa chạy thật cho tới khi gặp action.

`Transformation`: thao tác tạo DataFrame mới như `select`, `filter`, `withColumn`.

`Action`: thao tác kích hoạt chạy thật như `count`, `show`, `collect`, `write`.

`cache`: giữ DataFrame trong memory để dùng lại.

`persist`: giống cache nhưng chọn được storage level.

`unpersist`: bỏ cache khi không cần dùng nữa.

## Lệnh chạy

```bash
python chapters/16_cache_persist/demo.py
python chapters/16_cache_persist/exercise.py
```

## Nhớ nhanh

- DataFrame dùng nhiều lần thì cân nhắc cache.
- Không cache DataFrame chỉ dùng một lần.
- Cache sai chỗ có thể làm tốn RAM và chậm hơn.
- Dùng xong nên `unpersist()`.
