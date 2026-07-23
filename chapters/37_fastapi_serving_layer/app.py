"""API serving tối giản bằng thư viện chuẩn, không cần dependency mới."""

import csv
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlparse


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CUSTOMER_FILE = PROJECT_ROOT / "data" / "input" / "customer.csv"


def load_customers() -> list[dict[str, str]]:
    """Đọc dữ liệu mẫu; production thường đọc Gold/Serving store."""
    with CUSTOMER_FILE.open(encoding="utf-8", newline="") as source_file:
        return list(csv.DictReader(source_file))


def province_summary(customers: list[dict[str, str]]) -> list[dict[str, object]]:
    summary: dict[str, int] = {}
    for customer in customers:
        province = customer.get("province_code") or "UNKNOWN"
        summary[province] = summary.get(province, 0) + 1
    return [{"province_code": key, "customer_count": value} for key, value in sorted(summary.items())]


class ServingHandler(BaseHTTPRequestHandler):
    def send_json(self, status_code: int, payload: object) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 - tên method do BaseHTTPRequestHandler yêu cầu.
        path = urlparse(self.path).path
        customers = load_customers()

        if path == "/health":
            self.send_json(200, {"status": "ok"})
        elif path == "/customers":
            self.send_json(200, customers)
        elif path.startswith("/customers/"):
            customer_id = path.rsplit("/", 1)[-1]
            customer = next((item for item in customers if item["customer_id"] == customer_id), None)
            self.send_json(200, customer) if customer else self.send_json(404, {"message": "Customer not found"})
        elif path == "/reports/province-summary":
            self.send_json(200, province_summary(customers))
        else:
            self.send_json(404, {"message": "Endpoint not found"})


if __name__ == "__main__":
    print("Serving API: http://localhost:8000")
    HTTPServer(("0.0.0.0", 8000), ServingHandler).serve_forever()
