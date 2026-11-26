# Water Quality Cloud Monitoring Pipeline (Seminar & BTL — Nhóm 12, L01)

Hệ thống thu thập, kiểm định, cảnh báo ngưỡng và trực quan hóa dữ liệu quan trắc chất lượng nước theo thời gian thực trên nền tảng **AWS EC2 (IaaS) + Neon Serverless PostgreSQL (Managed DB)**, sử dụng dữ liệu thực tế năm 2025 từ UK Environment Agency (`environment.data.gov.uk/water-quality`, `177.319` bản ghi).

## Kiến trúc 3 phân vùng hạ tầng

```mermaid
flowchart LR
    subgraph Z1["Vùng 1: Local Laptop (Client Edge)"]
        CSV["2025-C - Copy.csv"] --> PROD["replay_producer.py\n(key = station_id)"]
    end

    subgraph TUN["Kênh bảo mật"]
        SSH["SSH Tunnel\n127.0.0.1:19092 -> EC2:19092"]
    end

    subgraph Z2["Vùng 2: AWS EC2 t3.medium (Docker Compose)"]
        KAFKA["Kafka KRaft Broker\n(3 Partitions, 127.0.0.1:19092)"]
        CONS["Python Consumer\n(Validate, Filter, Alert, UNNEST Batch)"]
        GRAF["Grafana 11.3\n(Port 3000 Web UI)"]
        KAFKA --> CONS
    end

    subgraph Z3["Vùng 3: Neon Cloud (Managed Serverless PostgreSQL 16)"]
        NEON[("Neon DB\nsslmode=require\nPK (run_id, event_id)")]
    end

    PROD --> SSH --> KAFKA
    CONS -->|"TLS 5432 + ON CONFLICT DO NOTHING"| NEON
    GRAF -->|"SQL Query (TLS 5432)"| NEON
```

## Cấu trúc thư mục

```text
water-quality-cloud/
├── compose.yaml                     # Stack chạy trên AWS EC2 (Kafka KRaft, Consumer, Grafana -> Neon)
├── compose.local.yaml               # Override chạy offline hoàn toàn bằng PostgreSQL Docker local
├── .env.example                     # Mẫu biến môi trường (12-Factor config)
├── memory.md                        # Bộ nhớ dự án: tiến độ đã làm & quy tắc bắt buộc
├── data/
│   └── 2025-C - Copy.csv            # Bộ dữ liệu CSV 2025 đang sử dụng (gitignored, kèm .gitkeep)
├── db/
│   ├── schema.sql                   # Schema: water_measurements, rejected_events, pipeline_runs
│   └── init_db.py                   # Script khởi tạo & kiểm tra 3 bảng trên Neon PostgreSQL
├── producer/
│   ├── replay_producer.py           # Đọc CSV 2025 từ data/, phát JSON vào Kafka với rate limiter & report JSON
│   └── requirements.txt
├── consumer/
│   ├── Dockerfile                   # Non-root Python 3.12 container
│   ├── requirements.txt
│   └── app/
│       ├── validator.py             # Lọc NO FLOW/SAMP, kiểm tra miền vật lý, gắn alert NORMAL/WARNING/CRITICAL
│       ├── db.py                    # Batch UNNEST + ON CONFLICT DO NOTHING + retry reconnect Neon
│       └── main.py                  # Kafka consumer manual commit + hook test crash (exit code 77)
├── grafana/provisioning/
│   ├── datasources/neon-postgres.yaml
│   └── dashboards/
│       ├── dashboards.yaml
│       └── json/water-quality-monitoring.json
├── benchmarks/
│   ├── evaluate_run.py              # Đo throughput, p50/p95/max latency, trùng lặp và Kafka lag
│   ├── generate_charts.py           # Sinh ảnh PNG dark-theme chuẩn slide từ JSON kết quả Cloud
│   ├── generate_canva_assets.py     # Sinh 4 ảnh PNG nền sáng cho slide Canva
│   └── run_seminar_benchmarks.ps1   # Tự động hóa 4 bài test Seminar qua SSH tunnel
├── tests/                           # Unit & smoke tests (pytest)
└── docs/
    ├── BEGINNER_SYSTEM_AND_SLIDE_GUIDE.md   # Giải thích từ số 0 cho người mới & đối chiếu 19 slide Canva
    ├── OFFLINE_ONLINE_STEP_BY_STEP_GUIDE.md # Hướng dẫn chi tiết từng bước chạy Offline & Online
    ├── OFFLINE_ONLINE_EXECUTION_FLOW.md     # Giải thích luồng hoạt động chi tiết khi chạy Offline & Online
    ├── BTL_CLOUD_NATIVE_ROADMAP.md          # Lộ trình Bài tập lớn 12/12 (Nhóm 1 & Nhóm 2)
    └── CLOUD_VERIFICATION_REPORT.md         # Báo cáo số liệu đo thật trên AWS EC2 & Neon DB (gitignored)
```

## Quickstart nhanh

### 1. Kiểm tra offline trên máy cá nhân (tùy chọn)

```powershell
cd water-quality-cloud
copy .env.example .env
docker compose -f compose.yaml -f compose.local.yaml up -d --build
python -m venv .venv
.\.venv\Scripts\pip install -r producer\requirements.txt -r benchmarks\requirements.txt
.\.venv\Scripts\python producer\replay_producer.py --rate 20 --max-events 200
```

Mở Grafana tại `http://localhost:3000` (user/password trong `.env`).

Xem hướng dẫn đầy đủ cho cả hai chế độ **Offline** & **Online** tại [OFFLINE_ONLINE_STEP_BY_STEP_GUIDE.md](docs/OFFLINE_ONLINE_STEP_BY_STEP_GUIDE.md) và giải thích luồng hoạt động tại [OFFLINE_ONLINE_EXECUTION_FLOW.md](docs/OFFLINE_ONLINE_EXECUTION_FLOW.md).

### 2. Triển khai thật lên AWS EC2 + Neon PostgreSQL

Xem hướng dẫn chi tiết từng lệnh copy-paste trong phần **Online** của [OFFLINE_ONLINE_STEP_BY_STEP_GUIDE.md](docs/OFFLINE_ONLINE_STEP_BY_STEP_GUIDE.md).
