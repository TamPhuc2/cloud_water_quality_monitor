# Hướng dẫn Chi tiết Từng bước Chạy Pipeline OFFLINE & ONLINE

Tài liệu này hướng dẫn đầy đủ từ con số 0 cách cài đặt, khởi chạy, kiểm thử và xử lý sự cố cho hệ thống **Water Quality Cloud Monitoring Pipeline** (`water-quality-cloud/`) ở **cả 2 chế độ**:

- **Chế độ OFFLINE (Local Docker)**: Chạy toàn bộ 100% trên máy cá nhân bằng Docker Compose override (`compose.yaml` + `compose.local.yaml`), không cần Internet, không tốn chi phí AWS/Neon.
- **Chế độ ONLINE (Cloud Production — AWS EC2 + Neon Serverless PostgreSQL + SSH Tunnel)**: Triển khai thực tế trên 3 phân vùng hạ tầng (Laptop Edge → SSH Tunnel → AWS EC2 → Neon Cloud), dùng để chạy demo live và đo benchmark WAN.

> **Lưu ý bảo mật:** Toàn bộ địa chỉ IP, tên endpoint Neon, tên người dùng DB và đường dẫn khóa SSH `.pem` trong tài liệu này sử dụng ký hiệu thay thế (`<EC2_IP>`, `<NEON_HOST>`, `<NEON_USER>`, `<NEON_PASSWORD>`, `<PATH_TO_PEM>`). Tuyệt đối không đưa giá trị thật vào các file theo dõi trên Git.

---

## Bảng so sánh nhanh: OFFLINE vs ONLINE

| Tiêu chí | Chế độ OFFLINE (Local) | Chế độ ONLINE (Cloud) |
| :--- | :--- | :--- |
| **Mục đích** | Phát triển, debug code, kiểm thử logic lọc/cảnh báo/crash recovery không tốn credit | Demo live trước GVHD, đo độ trễ mạng WAN, benchmark burst/single vs batch/crash |
| **Hạ tầng chạy stack** | Máy Windows/Linux cá nhân (Docker Desktop) | Máy ảo **AWS EC2** (`c7i-flex.large` hoặc `t3.medium`, Ubuntu 24.04) |
| **Lệnh Docker Compose** | `docker compose -f compose.yaml -f compose.local.yaml up -d --build` | `docker compose up -d --build` (trên EC2) |
| **Số container chạy** | **5** (`wq-local-postgres`, `wq-kafka`, `wq-kafka-init`, `wq-consumer`, `wq-grafana`) | **4** (`wq-kafka`, `wq-kafka-init`, `wq-consumer`, `wq-grafana`) |
| **Cơ sở dữ liệu & TLS** | PostgreSQL 16 Docker nội bộ (`postgres:5432`, `sslmode=disable`) | **Neon Serverless PostgreSQL 16** (`<NEON_HOST>:5432`, `sslmode=require`) |
| **Khởi tạo schema** | Tự động nạp `db/schema.sql` qua `/docker-entrypoint-initdb.d/01-schema.sql` | Chạy chủ động 1 lần bằng `python db/init_db.py` |
| **Cần SSH Tunnel?** | **Không** (Producer nối thẳng Kafka qua `127.0.0.1:19092`) | **Bắt buộc** (`ssh -i "<PATH_TO_PEM>" -N -L 19092:127.0.0.1:19092 ubuntu@<EC2_IP>`) |
| **Cổng mở trên host/Internet** | Host chỉ mở `127.0.0.1:19092` và `3000` (Postgres `5432` **không** publish ra host) | EC2 Security Group chỉ mở `22` (My IP) và `3000` (My IP); `9092/19092` đóng hoàn toàn |
| **URL Grafana** | `http://localhost:3000/d/wq-cloud-n12-l01` | `http://<EC2_IP>:3000/d/wq-cloud-n12-l01` |
| **Cách kiểm tra DB** | `docker exec -it wq-local-postgres psql -U water_user -d water_db ...` | Chạy `benchmarks/evaluate_run.py` từ Laptop hoặc truy vấn SQL trực tiếp tới Neon |

---

## Phần 0 — Chuẩn bị chung trên máy cá nhân (Windows — chỉ làm 1 lần)

### 0.1. Kiểm tra công cụ bắt buộc

Mở PowerShell và kiểm tra các công cụ sau đã cài và hoạt động:

```powershell
docker --version
docker compose version
python --version
git --version
ssh -V
```

- **Docker Desktop** phải đang mở (*Engine running*).
- **Python 3.12+** (đã xác nhận chạy tốt với Python 3.13 trên Windows).
- **OpenSSH Client** (có sẵn trên Windows 10/11) để dùng lệnh `ssh` và `scp` khi chạy Online.

### 0.2. Kiểm tra dữ liệu CSV trong `water-quality-cloud/data/`

Tập dữ liệu đã được sao chép sẵn vào thư mục chuẩn `water-quality-cloud/data/`:

```powershell
cd "d:\HCMUT\HK261\Cloud Computing\Cloud_Assignment\water-quality-cloud"
Get-ChildItem .\data -Force
```

Kỳ vọng thấy:
- `2025-C - Copy.csv` (~62,1 MB, `177.319` bản ghi thực tế năm 2025 — bị ẩn khỏi Git nhờ luật `*.csv` trong `.gitignore`).
- `2025-C_sample.csv` (~5,2 KB, `14` bản ghi mẫu phục vụ unit test và fallback).
- `.gitkeep` (giữ thư mục `data/` trên Git).

Hàm `default_csv_path()` trong `producer/replay_producer.py` sẽ tự động ưu tiên đọc `water-quality-cloud/data/2025-C - Copy.csv` trước, không cần truyền `--csv` thủ công.

### 0.3. Tạo môi trường ảo Python `.venv` và chạy Unit Tests kiểm chứng

Nếu chưa có `.venv`, tạo mới và cài đặt các thư viện cần thiết cho Producer, DB init và Benchmark:

```powershell
cd "d:\HCMUT\HK261\Cloud Computing\Cloud_Assignment\water-quality-cloud"
python -m venv .venv
.\.venv\Scripts\python -m pip install --upgrade pip
.\.venv\Scripts\pip install -r producer\requirements.txt -r benchmarks\requirements.txt
```

Chạy bộ kiểm thử tự động để xác nhận logic Producer, Validator, `UNNEST` DB writer, Evaluation và Chart generator đều hoạt động đúng trước khi chạy stack:

```powershell
.\.venv\Scripts\pytest tests -v
```

Kỳ vọng tất cả bài test đều `PASSED` (ví dụ `11 passed`).

> [!WARNING]
> **Cảnh báo xung đột cổng `127.0.0.1:19092`:**
> Cả **Kafka Offline** (chạy trong Docker trên laptop) và **SSH Tunnel Online** (forward tới Kafka trên EC2) đều dùng chung cổng `127.0.0.1:19092` trên laptop.
> **Tuyệt đối không chạy cả hai cùng lúc.** Trước khi chuyển từ Offline sang Online, luôn chạy `docker compose -f compose.yaml -f compose.local.yaml down`. Ngược lại, trước khi chuyển từ Online sang Offline, nhớ tắt cửa sổ SSH Tunnel (`Ctrl+C`).

---

## Phần 1 — Hướng dẫn từng bước chạy OFFLINE (100% cục bộ)

Chế độ Offline thay thế Neon Cloud bằng container `postgres:16-alpine` (`wq-local-postgres`) nằm chung mạng nội bộ Docker `water_net` với Kafka, Consumer và Grafana.

### Bước 1.1 — Đảm bảo không còn SSH Tunnel hay cổng `19092` bị chiếm

Kiểm tra nhanh trên PowerShell xem cổng `19092` và `3000` đang rảnh:

```powershell
Get-NetTCPConnection -LocalPort 19092,3000 -ErrorAction SilentlyContinue
```

Nếu thấy tiến trình `ssh.exe` đang chiếm cổng `19092`, hãy tắt cửa sổ PowerShell đang chạy SSH Tunnel.

### Bước 1.2 — Tạo file cấu hình `.env`

```powershell
cd "d:\HCMUT\HK261\Cloud Computing\Cloud_Assignment\water-quality-cloud"
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
```

Mở `.env` nếu muốn đổi mật khẩu Grafana (`GRAFANA_ADMIN_USER`, `GRAFANA_ADMIN_PASSWORD`) hoặc chế độ Consumer (`CONSUMER_MODE=batch`, `CONSUMER_BATCH_SIZE=100`).

> **Lưu ý:** Ở chế độ Offline, các dòng placeholder `NEON_HOST=...` và `DATABASE_URL=...` trong `.env` **không cần sửa**. File `compose.local.yaml` tự động ghi đè `DATABASE_URL` của `consumer` thành `postgresql://water_user:water_local_pass@postgres:5432/water_db?sslmode=disable` và ghi đè datasource của `grafana` sang `postgres` với `PG_SSLMODE=disable`.
> Tuy nhiên, vẫn phải giữ `CONSUMER_CRASH_AFTER_DB_COMMIT=false` để Consumer không tự crash khi khởi động.

### Bước 1.3 — Khởi động toàn bộ stack Offline

```powershell
cd "d:\HCMUT\HK261\Cloud Computing\Cloud_Assignment\water-quality-cloud"
docker compose -f compose.yaml -f compose.local.yaml up -d --build
```

Lệnh này thực hiện:
1. Khởi tạo `wq-local-postgres` và tự động chạy `db/schema.sql` (nếu volume `local_pg_data` mới tạo lần đầu).
2. Khởi động `wq-kafka` (KRaft mode, không dùng Zookeeper) và đợi healthcheck xanh.
3. Chạy `wq-kafka-init` một lần để tạo topic `water-quality-events` với `3` partitions.
4. Build image Python 3.12 non-root cho `wq-consumer` và khởi động vòng lặp đọc Kafka.
5. Khởi động `wq-grafana` kèm datasource và dashboard 10 panels đã provision sẵn.

### Bước 1.4 — Kiểm tra sức khỏe các container và Schema DB

```powershell
docker compose -f compose.yaml -f compose.local.yaml ps
docker logs wq-kafka-init
docker logs --tail 20 wq-consumer
```

Kiểm tra trạng thái kỳ vọng:
- `wq-local-postgres`: `Up (healthy)`
- `wq-kafka`: `Up (healthy)`
- `wq-kafka-init`: `Exited (0)` — log có dòng `Topic: water-quality-events ... PartitionCount: 3`
- `wq-consumer`: `Up` — log JSON có `"event": "CONSUMER_STARTED"` với `"consumer_mode": "batch"`
- `wq-grafana`: `Up`

Xác minh 3 bảng đã được tạo trong PostgreSQL local:

```powershell
docker exec -it wq-local-postgres psql -U water_user -d water_db -c "\dt"
```

Kỳ vọng hiển thị đủ 3 bảng: `pipeline_runs`, `rejected_events`, `water_measurements`.

### Bước 1.5 — Chạy Producer đẩy dữ liệu CSV vào pipeline Offline

**Cách A — Chạy thử nghiệm 200 sự kiện với tốc độ 20 sự kiện/giây:**

```powershell
cd "d:\HCMUT\HK261\Cloud Computing\Cloud_Assignment\water-quality-cloud"
.\.venv\Scripts\python producer\replay_producer.py --rate 20 --max-events 200 --run-id offline-smoke-1
```

**Cách B — Chạy liên tục (vòng lặp) để xem biểu đồ Grafana trôi theo thời gian thực:**

```powershell
.\.venv\Scripts\python producer\replay_producer.py --rate 25 --loop --run-id offline-live-demo
```

*(Nhấn `Ctrl+C` để dừng an toàn; Producer sẽ tự động `flush()` hết tin nhắn còn trong buffer trước khi thoát.)*

**Tóm tắt các tham số CLI của `producer/replay_producer.py`:**

| Tham số | Mặc định | Ý nghĩa |
| :--- | :--- | :--- |
| `--csv` | `data/2025-C - Copy.csv` | Đường dẫn file CSV đầu vào |
| `--bootstrap` | `127.0.0.1:19092` | Địa chỉ Kafka broker (đọc từ `KAFKA_BOOTSTRAP_SERVERS` trong `.env`) |
| `--topic` | `water-quality-events` | Tên Kafka topic đích |
| `--run-id` | `run-<YYYYMMDD-HHMMSS>` (UTC) | Mã phiên chạy để phân biệt trên DB và biến `$run_id` của Grafana |
| `--rate` | `20.0` | Số sự kiện/giây (`0` = bắn burst tối đa không giới hạn) |
| `--max-events` | `0` | Giới hạn số bản ghi đẩy lên (`0` = hết toàn bộ file CSV hoặc vô hạn nếu kèm `--loop`) |
| `--core-only` / `--all-determinands` | `--core-only` (`True`) | Mặc định lọc 8 chỉ tiêu cốt lõi (`BOD ATU`, `Sld Sus@105C`, `Ammonia(N)`, `COD as O2`, `Phosphorus-P`, `pH`, `Temp Water`, `NO FLOW/SAMP`); dùng `--all-determinands` để đẩy toàn bộ 483 chỉ tiêu |
| `--loop` | `False` | Đọc xoay vòng liên tục (tự thêm hậu tố `-loop1`, `-loop2` vào `event_id` để tránh trùng khóa khi lặp) |
| `--report-json` | `None` | Lưu file JSON báo cáo tổng kết phiên gửi của Producer |

### Bước 1.6 — Quan sát Dashboard Grafana ở chế độ Offline

1. Mở trình duyệt tới: **`http://localhost:3000/d/wq-cloud-n12-l01`**
2. Đăng nhập bằng tài khoản cấu hình trong `.env` (`GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD`; mặc định nếu chưa sửa trong `.env.example` là `admin` / `CHANGE_ME_GRAFANA_PASSWORD`).
3. Phía trên dashboard:
   - **`run_id`**: chọn `All` hoặc chọn riêng `offline-smoke-1` / `offline-live-demo`.
   - **`determinand`**: chọn `All` hoặc lọc từng chỉ tiêu (`BOD ATU`, `Ammonia(N)`, `pH`,...).
4. Kiểm tra tự động làm mới (`Auto-refresh` góc phải đặt `5s`). Quan sát đầy đủ 10 panel: tổng bản ghi hợp lệ, bản ghi nhiễu bị loại (`NO FLOW/SAMP`), số cảnh báo (`WARNING` / `CRITICAL`), P95 Latency, Throughput ops/s, biểu đồ live theo `ingested_at` và phân bố lịch sử 2025 theo `event_time`.

### Bước 1.7 — Kiểm tra log Consumer và dữ liệu DB nội bộ

Xem log JSON của Consumer theo thời gian thực:

```powershell
docker logs -f wq-consumer
```

Kiểm tra trực tiếp số dòng và kiểm chứng không có dòng trùng trong PostgreSQL local (chú ý: vì `compose.local.yaml` bảo mật không mở cổng `5432` ra host, ta truy vấn trực tiếp qua `docker exec`):

```powershell
docker exec -i wq-local-postgres psql -U water_user -d water_db -c "
SELECT run_id,
       COUNT(*) AS valid_rows,
       COUNT(DISTINCT event_id) AS distinct_events,
       COUNT(*) - COUNT(DISTINCT event_id) AS duplicates
FROM water_measurements
GROUP BY run_id
ORDER BY MAX(ingested_at) DESC;
"
```

Kiểm tra các bản ghi bị từ chối (`NO FLOW/SAMP`, giá trị lỗi,...) và phân bố cảnh báo:

```powershell
docker exec -i wq-local-postgres psql -U water_user -d water_db -c "
SELECT reason, COUNT(*) FROM rejected_events GROUP BY reason ORDER BY COUNT(*) DESC;
SELECT alert_level, COUNT(*) FROM water_measurements GROUP BY alert_level;
"
```

### Bước 1.8 — (Tùy chọn) Thử nghiệm chế độ `single` và Crash Recovery (`exit code 77`) Offline

**Thử nghiệm Crash Recovery an toàn (dùng container one-shot `--rm` để Docker không tự restart vòng lặp):**

```powershell
# 1. Dừng consumer bình thường
docker compose -f compose.yaml -f compose.local.yaml stop consumer

# 2. Mở 1 consumer one-shot có cài bẫy crash ngay sau khi commit DB, trước khi commit Kafka offset
# (Chạy lệnh này ở PowerShell A, cửa sổ sẽ tự thoát với mã lỗi 77 khi nhận sự kiện đầu tiên)
docker compose -f compose.yaml -f compose.local.yaml run --rm `
  -e CONSUMER_MODE=single `
  -e CONSUMER_CRASH_AFTER_DB_COMMIT=true `
  -e CONSUMER_CRASH_TARGET_INDEX=0 `
  consumer
```

Ở PowerShell B, bơm 100 sự kiện:

```powershell
.\.venv\Scripts\python producer\replay_producer.py --rate 20 --max-events 100 --run-id offline-crash-test
```

Quan sát PowerShell A: log in ra `"event": "INJECTED_CRASH_AFTER_DB_COMMIT", "exit_code": 77, "offset_committed": false`.

Khôi phục Consumer bình thường để Kafka phát lại offset chưa commit và xem cơ chế chống trùng:

```powershell
docker compose -f compose.yaml -f compose.local.yaml up -d consumer
docker logs --tail 25 wq-consumer
```

Sau đó kiểm tra lại bảng `water_measurements` và `rejected_events` với `run_id = 'offline-crash-test'`: tổng số bản ghi đúng bằng `100` (`valid + rejected`), số dòng trùng `duplicates = 0`. *(Lưu ý: `wq-consumer` in log `DEDUP_REPLAY` khi bản ghi replay là bản ghi hợp lệ `deduped_valid > 0`; nếu sự kiện đầu bị crash là `NO FLOW/SAMP` vào `rejected_events`, `ON CONFLICT DO NOTHING` vẫn tự bỏ qua bản trùng âm thầm và `BATCH_COMMITTED` sẽ báo `inserted_rejected` giảm đi 1.)*

### Bước 1.9 — Dừng và dọn dẹp môi trường Offline

- **Tắt container nhưng GIỮ lại dữ liệu DB, Kafka và Grafana:**
  ```powershell
  docker compose -f compose.yaml -f compose.local.yaml down
  ```
- **Tắt container VÀ XÓA SẠCH dữ liệu để làm lại từ đầu** *(chỉ dùng khi muốn reset hoàn toàn; `01-schema.sql` sẽ chạy lại từ đầu)*:
  ```powershell
  docker compose -f compose.yaml -f compose.local.yaml down -v
  ```

---

## Phần 2 — Hướng dẫn từng bước chạy ONLINE (AWS EC2 + Neon Cloud + SSH Tunnel)

Chế độ Online triển khai kiến trúc 3 phân vùng hoàn chỉnh trên Internet:
**Laptop Edge** → **SSH Tunnel (Port 22)** → **AWS EC2 (Kafka KRaft + Consumer + Grafana)** → **Neon Serverless PostgreSQL 16 (TLS 5432)**.

### Bước 2.1 — Tắt stack Offline trên laptop (nếu trước đó đã chạy)

```powershell
cd "d:\HCMUT\HK261\Cloud Computing\Cloud_Assignment\water-quality-cloud"
docker compose -f compose.yaml -f compose.local.yaml down
```

Mục đích: giải phóng cổng `127.0.0.1:19092` cho SSH Tunnel và cổng `3000` trên máy cá nhân.

### Bước 2.2 — Khởi tạo Neon Serverless PostgreSQL 16 & nạp Schema

1. Đăng nhập [https://console.neon.tech](https://console.neon.tech) và tạo Project:
   - **Postgres version**: `16`
   - **Region**: khuyến nghị `AWS Asia Pacific (Singapore) - ap-southeast-1` (hoặc region gần EC2).
2. Vào **Connection Details**, bật tùy chọn **Pooled connection** (endpoint có chứa `-pooler`). Ghi lại:
   - `<NEON_HOST>` (dạng `ep-...-pooler.<region>.aws.neon.tech`)
   - `<NEON_DB>` (thường là `neondb`)
   - `<NEON_USER>` (thường là `neondb_owner`)
   - `<NEON_PASSWORD>`
   - Chuỗi kết nối đầy đủ có `sslmode=require`:
     ```text
     postgresql://<NEON_USER>:<NEON_PASSWORD>@<NEON_HOST>/<NEON_DB>?sslmode=require
     ```
3. Mở file `.env` trên Laptop và điền các giá trị thật vào `NEON_HOST`, `NEON_DB`, `NEON_USER`, `NEON_PASSWORD`, `DATABASE_URL`, `GRAFANA_ADMIN_PASSWORD`.
4. Khởi tạo 3 bảng trên Neon từ Laptop:
   ```powershell
   cd "d:\HCMUT\HK261\Cloud Computing\Cloud_Assignment\water-quality-cloud"
   .\.venv\Scripts\python db\init_db.py
   ```
   Kết quả kỳ vọng:
   `Verified tables (3/3): pipeline_runs, rejected_events, water_measurements`
   `Database initialization completed successfully.`

### Bước 2.3 — Khởi tạo AWS EC2 & cấu hình Security Group

1. Vào **AWS Console → EC2 → Launch Instance**:
   - **AMI**: `Ubuntu Server 24.04 LTS (HVM), SSD Volume Type`
   - **Instance type**: `c7i-flex.large` hoặc `t3.medium` (**2 vCPU, 4 GiB RAM** — tránh `t3.micro` 1 GB RAM vì Kafka JVM rất dễ bị OOM kill).
   - **Storage**: `20 GiB gp3`
   - **Key pair**: tải file `.pem` về máy cá nhân (lưu ngoài thư mục repo, ví dụ tại `<PATH_TO_PEM>`).
2. Cấu hình **Inbound Rules của Security Group** theo nguyên tắc tối thiểu đặc quyền:

| Type | Protocol | Port | Source | Ghi chú |
| :--- | :--- | :--- | :--- | :--- |
| SSH | TCP | `22` | **My IP** | Dùng cho SSH quản trị và mở SSH Tunnel |
| Custom TCP | TCP | `3000` | **My IP** (khi demo trên lớp: thêm IP mạng trường hoặc mở tạm rồi đóng ngay sau demo) | Truy cập Grafana Web UI |
| *(Không thêm rule)* | — | `9092`, `19092` | **Chặn toàn bộ Internet** | Kafka chỉ bind `127.0.0.1:19092` |

3. Ghi lại địa chỉ **Public IPv4** của máy ảo: `<EC2_IP>`.

### Bước 2.4 — Cài đặt Docker trên EC2, đưa mã nguồn lên và khởi chạy

Mở PowerShell và SSH vào EC2:

```powershell
ssh -i "<PATH_TO_PEM>" ubuntu@<EC2_IP>
```

**Trên EC2**, cài đặt Docker Engine và Docker Compose plugin (chỉ làm 1 lần khi mới tạo EC2):

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl git
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker ubuntu
newgrp docker
```

Đưa mã nguồn `water-quality-cloud` lên EC2:
- **Cách 1 (khuyên dùng nếu đã push Git):**
  ```bash
  git clone <URL_REPO_CUA_BAN> ~/water-quality-cloud
  ```
- **Cách 2 (copy trực tiếp từ PowerShell trên Laptop bằng `scp` — lưu ý không cần copy file CSV 62 MB hay `.venv` lên EC2 vì Producer chạy trên Laptop):**
  ```powershell
  ssh -i "<PATH_TO_PEM>" ubuntu@<EC2_IP> "mkdir -p ~/water-quality-cloud"
  scp -i "<PATH_TO_PEM>" -r "d:\HCMUT\HK261\Cloud Computing\Cloud_Assignment\water-quality-cloud\consumer" `
    "d:\HCMUT\HK261\Cloud Computing\Cloud_Assignment\water-quality-cloud\db" `
    "d:\HCMUT\HK261\Cloud Computing\Cloud_Assignment\water-quality-cloud\grafana" `
    "d:\HCMUT\HK261\Cloud Computing\Cloud_Assignment\water-quality-cloud\compose.yaml" `
    "d:\HCMUT\HK261\Cloud Computing\Cloud_Assignment\water-quality-cloud\.env.example" `
    ubuntu@<EC2_IP>:~/water-quality-cloud/
  ```

**Trên EC2**, tạo `.env` và khởi động stack Cloud (dùng `compose.yaml` thuần, **không** dùng `-f compose.local.yaml`):

```bash
cd ~/water-quality-cloud
cp .env.example .env
nano .env
```

Kiểm tra các giá trị quan trọng trong `.env` trên EC2:
- `NEON_HOST`, `NEON_DB`, `NEON_USER`, `NEON_PASSWORD`, `PG_SSLMODE=require`, `DATABASE_URL` (khớp Neon)
- `GRAFANA_ADMIN_USER`, `GRAFANA_ADMIN_PASSWORD` (mật khẩu mạnh)
- `CONSUMER_MODE=batch`
- `CONSUMER_BATCH_SIZE=100`
- `CONSUMER_BATCH_TIMEOUT_MS=500`
- `CONSUMER_CRASH_AFTER_DB_COMMIT=false`

Khởi động stack:

```bash
docker compose up -d --build
```

### Bước 2.5 — Kiểm tra container và xác minh cổng bind trên EC2

Trên EC2 chạy:

```bash
docker compose ps
docker logs wq-kafka-init
docker logs --tail 20 wq-consumer
ss -tlnp | grep -E "19092|3000"
```

Kết quả kỳ vọng:
- `wq-kafka` (`Up (healthy)`), `wq-kafka-init` (`Exited (0)`, `PartitionCount: 3`), `wq-consumer` (`Up`, log `CONSUMER_STARTED`), `wq-grafana` (`Up`).
- `ss -tlnp` cho thấy cổng `19092` chỉ lắng nghe trên `127.0.0.1:19092` (tuyệt đối không phải `0.0.0.0:19092`), còn cổng `3000` lắng nghe để phục vụ trình duyệt.

### Bước 2.6 — Mở SSH Tunnel trên Laptop (PowerShell 1)

Mở **Cửa sổ PowerShell 1** trên Laptop và giữ cửa sổ này mở suốt thời gian chạy Producer hoặc Benchmark:

```powershell
ssh -i "<PATH_TO_PEM>" -N -L 19092:127.0.0.1:19092 ubuntu@<EC2_IP>
```

*(Lệnh `-N` không mở shell tương tác nên con trỏ sẽ đứng yên — đó là trạng thái bình thường của tunnel đang hoạt động. Nếu muốn kiểm tra đường hầm vừa mở từ PowerShell khác, chạy `Test-NetConnection -ComputerName 127.0.0.1 -Port 19092` phải ra `TcpTestSucceeded : True`.)*

### Bước 2.7 — Chạy Producer trên Laptop đẩy dữ liệu CSV 2025 lên Cloud (PowerShell 2)

Mở **Cửa sổ PowerShell 2** trên Laptop:

```powershell
cd "d:\HCMUT\HK261\Cloud Computing\Cloud_Assignment\water-quality-cloud"

# 1. Smoke test 200 sự kiện lên Cloud
.\.venv\Scripts\python producer\replay_producer.py --rate 20 --max-events 200 --run-id demo-cloud-smoke

# 2. Hoặc chạy Demo Live liên tục (khi thuyết trình)
.\.venv\Scripts\python producer\replay_producer.py --rate 25 --loop --run-id demo-class-live
```

Nếu muốn đánh giá số liệu ngay sau lượt smoke test bằng `evaluate_run.py`:

```powershell
.\.venv\Scripts\python benchmarks\evaluate_run.py `
  --run-id demo-cloud-smoke `
  --phase smoke `
  --bootstrap 127.0.0.1:19092
```

*(Đảm bảo `.env` trên Laptop đã điền đúng `DATABASE_URL` của Neon để `evaluate_run.py` truy vấn thống kê trên Neon và đo `kafka_lag` qua SSH Tunnel.)*

### Bước 2.8 — Mở Grafana Cloud trên trình duyệt

Truy cập: **`http://<EC2_IP>:3000/d/wq-cloud-n12-l01`**

Đăng nhập bằng `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` đã đặt trên EC2. Kiểm tra 10 panel cập nhật trực tiếp từ dữ liệu trên Neon Cloud.

### Bước 2.9 — Chạy trọn bộ 4 Minh chứng Benchmark Seminar trên Cloud

Giữ **PowerShell 1 (SSH Tunnel)** đang chạy, ở **PowerShell 2** chạy:

```powershell
cd "d:\HCMUT\HK261\Cloud Computing\Cloud_Assignment\water-quality-cloud"
.\benchmarks\run_seminar_benchmarks.ps1 -Phase all
```

*(Hoặc chạy lẻ từng pha bằng `-Phase burst`, `-Phase single`, `-Phase batch`, `-Phase crash`, `-Phase charts`.)*

Script sẽ dừng và nhắc bạn thao tác trên EC2 đúng 3 điểm chuyển đổi:
1. **Pha Burst (10/s, 50/s, 100/s — mỗi lượt 300 sự kiện)**: đảm bảo `.env` trên EC2 đang để `CONSUMER_MODE=batch`.
2. **Pha Sustained Single-Commit (1.000 sự kiện @ 100/s)**:
   - Trên EC2 sửa `.env` thành `CONSUMER_MODE=single`, sau đó chạy:
     ```bash
     docker compose up -d consumer
     ```
   - Nhấn `Enter` trên PowerShell 2 (pha này kéo dài vài phút do độ trễ RTT WAN từng dòng).
3. **Pha Sustained Batch-Commit (1.000 sự kiện @ 100/s, batch=100)**:
   - Trên EC2 sửa `.env` lại thành `CONSUMER_MODE=batch`, sau đó chạy:
     ```bash
     docker compose up -d consumer
     ```
   - Nhấn `Enter` trên PowerShell 2.
4. **Pha Crash Recovery (`exit code 77`)**:
   - Trên EC2 dừng consumer thường và chạy container one-shot gài bẫy crash:
     ```bash
     docker compose stop consumer
     docker compose run --rm -e CONSUMER_MODE=single -e CONSUMER_CRASH_AFTER_DB_COMMIT=true -e CONSUMER_CRASH_TARGET_INDEX=0 consumer
     ```
   - Nhấn `Enter` để Producer gửi 100 sự kiện @ 20/s. Kiểm tra terminal EC2 đã thoát với **`exit code 77`** và log `INJECTED_CRASH_AFTER_DB_COMMIT`.
   - Khởi động lại consumer bình thường trên EC2 để replay offset chưa commit (`DEDUP_REPLAY` in ra khi `deduped_valid > 0`, còn nếu bản ghi đầu bị crash là `NO FLOW/SAMP` thì `ON CONFLICT DO NOTHING` chặn trùng trong `rejected_events` và `BATCH_COMMITTED` giảm `inserted_rejected` đi 1):
     ```bash
     docker compose up -d consumer
     docker logs --tail 25 wq-consumer
     ```
   - Nhấn `Enter` trên PowerShell 2 để `evaluate_run.py` xác nhận `duplicate_rows = 0` và `kafka_lag = 0`.
5. Cuối cùng script tự chạy `benchmarks/generate_charts.py` tạo 2 ảnh `slide9_cloud_throughput.png` và `slide10_cloud_recovery.png` trong `benchmarks/charts/` (có thể chạy thêm `.\.venv\Scripts\python benchmarks\generate_canva_assets.py` nếu cần bộ ảnh nền sáng cho Canva).

### Bước 2.10 — Kiểm chứng Bảo mật cổng mạng từ Internet

Chạy từ PowerShell trên Laptop (độc lập với SSH Tunnel):

```powershell
# Phải trả về TcpTestSucceeded : False (Kafka không lộ ra Internet)
Test-NetConnection -ComputerName <EC2_IP> -Port 19092
Test-NetConnection -ComputerName <EC2_IP> -Port 9092

# Phải trả về TcpTestSucceeded : True (chỉ mở từ IP được cấp phép trong Security Group)
Test-NetConnection -ComputerName <EC2_IP> -Port 3000
```

### Bước 2.11 — Quy trình Start / Stop để tiết kiệm AWS Credit

- **Sau khi dùng xong:** Vào AWS Console → EC2 → Instances → **Instance state → Stop instance** (tuyệt đối **không chọn Terminate**, vì Terminate sẽ xóa mất VM và EBS). Neon Free Tier sẽ tự động ngủ (*auto-suspend*) sau 5 phút không có truy vấn.
- **Trước khi chạy demo lại:**
  1. Bấm **Start instance** trên AWS Console.
  2. Lấy **Public IPv4** mới của EC2 (trừ khi đã gắn Elastic IP).
  3. Nếu bạn đổi mạng Wi-Fi (ví dụ mang laptop lên trường), vào **Security Group → Edit inbound rules** cập nhật lại nguồn **My IP** mới cho cổng `22` và `3000`.
  4. Chờ ~30–60 giây để Docker daemon trên EC2 tự khởi động lại `wq-kafka`, `wq-consumer`, `wq-grafana` (nhờ `restart: unless-stopped`), rồi mở lại SSH Tunnel với `<EC2_IP>` mới.

---

## Phần 3 — Xử lý lỗi thường gặp (Troubleshooting)

| Triệu chứng / Lỗi | Nguyên nhân gốc rễ | Cách khắc phục |
| :--- | :--- | :--- |
| Producer báo không kết nối được `127.0.0.1:19092` hoặc gửi bị treo | Đang chạy Online nhưng quên mở SSH Tunnel, hoặc đang bật cả Offline Docker Kafka lẫn SSH Tunnel gây đụng cổng `19092` | Tắt bớt một bên; nếu chạy Online kiểm tra `Test-NetConnection 127.0.0.1 -Port 19092` phải `True` |
| Đã sửa `GRAFANA_ADMIN_PASSWORD` trong `.env` nhưng đăng nhập Grafana vẫn báo sai mật khẩu | Grafana chỉ lấy mật khẩu khởi tạo từ env ở lần chạy đầu tiên; các lần sau lưu trong volume `grafana_data` | Giữ mật khẩu cũ hoặc chạy `docker exec -it wq-grafana grafana cli admin reset-admin-password <MAT_KHAU_MOI>` |
| Thay đổi `db/schema.sql` nhưng PostgreSQL Offline (`wq-local-postgres`) không cập nhật bảng | Script trong `/docker-entrypoint-initdb.d/` chỉ chạy khi volume `local_pg_data` trống | Chạy `docker compose -f compose.yaml -f compose.local.yaml down -v` rồi `up -d` lại (lưu ý mất dữ liệu local cũ) |
| Chạy `benchmarks/evaluate_run.py` khi đang dùng chế độ Offline bị lỗi kết nối DB | `compose.local.yaml` cố ý không mở cổng `5432` ra ngoài máy host, và `.env` mặc định đang trỏ về Neon | Để kiểm tra số liệu Offline, dùng `docker exec -i wq-local-postgres psql ...` như ở Bước 1.7; `evaluate_run.py` thiết kế cho Neon Cloud |
| Consumer log `OperationalError` vài giây đầu khi ghi lên Neon | Neon Serverless đang ở trạng thái ngủ (*auto-suspend*) và mất ~1–2 giây để cold-start compute | Bình thường: `DatabaseWriter` tự động retry exponential backoff (`1s, 2s, 4s, 8s`) và ghi tiếp thành công |
| Lỗi `DuplicatePreparedStatement` với Neon | Chưa tắt prepared statements khi đi qua PgBouncer pooled endpoint | Code `db.py` và `init_db.py` bắt buộc giữ `prepare_threshold=None` |
| Không SSH hoặc không mở được `http://<EC2_IP>:3000` khi đổi Wi-Fi hoặc khởi động lại EC2 | Public IPv4 của EC2 thay đổi sau Stop/Start, hoặc IP công cộng của laptop đổi so với rule **My IP** trong Security Group | Kiểm tra lại `<EC2_IP>` trên AWS Console và cập nhật **My IP** cho port `22` & `3000` |
