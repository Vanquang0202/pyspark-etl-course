# Chapter 37 - API Serving Layer

## Ghi chú học tập

Serving layer là lớp đưa dữ liệu đã được ETL xử lý đến ứng dụng khác. Demo này dùng HTTP server của Python chuẩn nên không cần cài FastAPI; các endpoint vẫn gần với API thực tế.

API trả dữ liệu theo từng request. BI/report thường phục vụ dashboard, biểu đồ hoặc báo cáo tổng hợp. Sau ETL, Gold/Serving có thể được API đọc và trả JSON cho web, mobile hoặc hệ thống khác.

## Endpoint

- `GET /health`
- `GET /customers`
- `GET /customers/{customer_id}`
- `GET /reports/province-summary`

Demo đọc `data/input/customer.csv`; production nên đọc bảng Serving, cache hoặc database phù hợp.

## Lệnh chạy

```bash
python chapters/37_fastapi_serving_layer/app.py
curl http://localhost:8000/health
curl http://localhost:8000/customers
curl http://localhost:8000/customers/001
curl http://localhost:8000/reports/province-summary
```

Không chạy server trong lúc chỉ đọc code. Nếu muốn đổi sang FastAPI sau này, cần thêm `fastapi` và `uvicorn` vào dependency.
