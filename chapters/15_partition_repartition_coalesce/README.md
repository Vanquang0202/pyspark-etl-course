# Chapter 15 - Partition, Repartition, Coalesce

## Bài này làm gì?

Bài này demo cách Spark chia dữ liệu thành nhiều partition để xử lý song song.
Mình học cách xem số partition, đổi số partition và giảm partition.
Bài cũng giải thích vì sao Spark ghi ra nhiều file `part-*`.
Phần demo/exercise có ghi output ra file.

## Học được gì?

- Partition là đơn vị Spark dùng để xử lý song song.
- `repartition()` dùng để tăng hoặc đổi số partition.
- `repartition()` thường có shuffle.
- `coalesce()` thường dùng để giảm số partition.
- Mỗi partition có thể ghi ra một file `part-*`.
- Có thể ghi output theo cột bằng `partitionBy()`.

## Khái niệm chính

`Partition`: một phần nhỏ của dữ liệu, Spark xử lý nhiều partition song song.

`repartition`: chia lại dữ liệu sang số partition mới, thường tốn shuffle.

`coalesce`: giảm số partition, hay dùng trước khi ghi file nhỏ hơn.

`part-*`: file output Spark tạo ra, thường tương ứng với partition khi ghi.

`partitionBy`: ghi dữ liệu ra folder theo giá trị của một cột.

## Lệnh chạy

```bash
python chapters/15_partition_repartition_coalesce/demo.py
python chapters/15_partition_repartition_coalesce/exercise.py
```

Trên Windows, nếu chưa setup Hadoop/winutils thì các bài có ghi output hoặc checkpoint có thể lỗi. Nên chạy bài này bằng Ubuntu/WSL.

## Nhớ nhanh

- Partition giúp Spark chạy song song.
- `repartition` đổi partition nhưng có shuffle.
- `coalesce` hay dùng để giảm partition.
- Nhiều partition thì dễ sinh nhiều file nhỏ.
- Spark output thường là folder, không phải một file.
