# Chapter 40 - Data Generator for ETL Testing

## Ghi chú học tập

ETL cần dữ liệu test để kiểm tra transform, volume và quality rule trước khi có source thật. Record valid xác nhận flow chuẩn; record invalid kiểm tra reject/quarantine và báo cáo chất lượng.

Generator tạo các cột `customer_id`, `name`, `province_code`, `amount`, `event_time`, `source_system`. Không cần dependency ngoài thư viện chuẩn.

## Lệnh chạy

```bash
python chapters/40_data_generator_for_etl_testing/generator.py \
  --rows 100 \
  --invalid-rate 0.1 \
  --output-path data/input/generated/customer_events.csv
```

Lệnh trên chỉ dùng khi muốn tự tạo file test; chapter này chưa chạy generator.
