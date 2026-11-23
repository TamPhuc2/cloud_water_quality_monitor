# Hướng dẫn triển khai AWS EC2 + Neon PostgreSQL (< 60 phút)

Hướng dẫn này thiết lập đúng kiến trúc Seminar:
- **Vùng 1 (Local Laptop)**: chạy `replay_producer.py` qua **SSH Tunnel** (không mở cổng Kafka ra Internet).
- **Vùng 2 (AWS EC2 `t3.medium`, Ubuntu 24.04)**: chạy Docker Compose gồm `kafka` (KRaft, 3 partitions), `consumer`, `grafana`.
- **Vùng 3 (Neon Serverless PostgreSQL 16)**: Managed DB có `sslmode=require`, vùng `ap-southeast-1` (Singapore).

---

## Bước 1 — Tạo Neon Serverless PostgreSQL (5 phút)

1. Đăng nhập [console.neon.tech](https://console.neon.tech), tạo Project mới:
   - **Postgres version**: `16`
   - **Region**: `AWS Asia Pacific (Singapore) - ap-southeast-1` (cùng region với EC2 để giảm RTT WAN nhưng vẫn là external managed DB).
2. Vào **Connection Details**, bật **Pooled connection**, chọn định dạng `psql` / connection string:
   ```text
   postgresql://<NEON_USER>:<NEON_PASSWORD>@<NEON_HOST>/neondb?sslmode=require
   ```
3. Ghi lại 4 thông số: `NEON_HOST` (có `-pooler`), `NEON_DB` (`neondb`), `NEON_USER`, `NEON_PASSWORD`, và chuỗi `DATABASE_URL` đầy đủ.

### Khởi tạo schema từ máy cá nhân (hoặc từ EC2)

```powershell
cd "d:\HCMUT\HK261\Cloud Computing\Cloud_Assignment\water-quality-cloud"
python -m venv .venv
.\.venv\Scripts\pip install -r producer\requirements.txt -r benchmarks\requirements.txt
copy .env.example .env
# Mở .env điền NEON_HOST, NEON_USER, NEON_PASSWORD, DATABASE_URL và GRAFANA_ADMIN_PASSWORD
.\.venv\Scripts\python db\init_db.py
```

Kết quả kỳ vọng:
`Verified tables (4/4): ml_predictions, pipeline_runs, rejected_events, water_measurements`.

---

## Bước 2 — Khởi tạo AWS EC2 (`t3.medium`) & Security Group (10 phút)

1. Vào **AWS Console → EC2 → Launch Instance** (Region: `ap-southeast-1` Singapore):
   - **Name**: `water-quality-seminar-n12`
   - **AMI**: `Ubuntu Server 24.04 LTS (HVM), SSD Volume Type`
   - **Instance type**: `t3.medium` (2 vCPU, 4 GB RAM — đủ chạy Kafka + Consumer + Grafana; nếu bắt buộc Free Tier có thể thử `t3.micro`/`t2.micro` với swap 2GB nhưng khuyến nghị `t3.medium` để không OOM khi demo).
   - **Key pair**: tạo mới hoặc chọn `.pem` sẵn có (lưu an toàn, không copy vào repo).
   - **Storage**: `20 GiB gp3`
2. **Network / Security Group** (chỉ mở đúng 2 cổng cần thiết):

| Type | Protocol | Port | Source | Mục đích |
| :--- | :--- | :--- | :--- | :--- |
| SSH | TCP | `22` | **My IP** | SSH quản trị & mở SSH tunnel cho Producer |
| Custom TCP | TCP | `3000` | **My IP** (lúc thuyết trình: thêm IP mạng trường hoặc tạm `0.0.0.0/0` rồi đóng sau demo) | Truy cập Grafana Web UI |
| *(Không mở)* | — | `9092 / 19092` | **Đóng hoàn toàn** | Kafka chỉ bind `127.0.0.1:19092`, chặn truy cập trực tiếp từ Internet |

3. Ghi lại **Public IPv4 address** của EC2 (ví dụ `<EC2_IP>`).

---

## Bước 3 — Cài Docker & chạy Pipeline trên EC2 (15 phút)

SSH vào EC2 từ PowerShell:

```powershell
ssh -i C:\path\to\your-key.pem ubuntu@<EC2_IP>
```

Trên EC2, cài Docker Engine + Docker Compose plugin:

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

Đưa thư mục `water-quality-cloud` lên EC2 (bằng `git clone` hoặc `scp -r` từ máy local):

```powershell
# Từ PowerShell máy local (nếu chưa đẩy lên Git):
scp -i C:\path\to\your-key.pem -r "d:\HCMUT\HK261\Cloud Computing\Cloud_Assignment\water-quality-cloud" ubuntu@<EC2_IP>:~/water-quality-cloud
```

Trên EC2, tạo `.env` và khởi động stack:

```bash
cd ~/water-quality-cloud
cp .env.example .env
nano .env
# Điền NEON_HOST, NEON_DB, NEON_USER, NEON_PASSWORD, DATABASE_URL, GRAFANA_ADMIN_PASSWORD
# Giữ CONSUMER_MODE=batch, CONSUMER_BATCH_SIZE=100, CONSUMER_CRASH_AFTER_DB_COMMIT=false

docker compose up -d --build
docker compose ps
docker logs wq-kafka-init
docker logs --tail 20 wq-consumer
```

Kiểm tra nhanh trên EC2:
- `wq-kafka` (healthy), `wq-kafka-init` (Exited 0, topic `water-quality-events` có `PartitionCount: 3`), `wq-consumer` (Up), `wq-grafana` (Up).
- Mở trình duyệt tới `http://<EC2_IP>:3000`, đăng nhập bằng `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD`, mở dashboard **Water Quality Cloud Monitoring (N12-L01)**.

---

## Bước 4 — Mở SSH Tunnel từ Laptop & chạy Demo Live

Mở **Cửa sổ PowerShell 1** (giữ mở suốt lúc chạy Producer):

```powershell
ssh -i C:\path\to\your-key.pem -N -L 19092:127.0.0.1:19092 ubuntu@<EC2_IP>
```

Mở **Cửa sổ PowerShell 2** để đẩy dữ liệu CSV 2025 thật lên Cloud:

```powershell
cd "d:\HCMUT\HK261\Cloud Computing\Cloud_Assignment\water-quality-cloud"
.\.venv\Scripts\python producer\replay_producer.py --rate 20 --loop
```

Quan sát trên Grafana (`http://<EC2_IP>:3000`): các thẻ KPI, Ingestion Rate, End-to-End Latency, biểu đồ chất lượng nước và bảng Latest Measurements cập nhật mỗi 5 giây. Nhấn `Ctrl+C` khi muốn dừng demo.

---

## Bước 5 — Chạy trọn bộ 4 Minh chứng Cloud cho Slide 9–11 (15 phút)

Giữ **SSH Tunnel** ở PowerShell 1 đang chạy, mở PowerShell 2:

```powershell
cd "d:\HCMUT\HK261\Cloud Computing\Cloud_Assignment\water-quality-cloud"
.\benchmarks\run_seminar_benchmarks.ps1 -Phase all
```

Script sẽ hướng dẫn tuần tự 4 pha:
1. **Burst 300 sự kiện @ 10/s, 50/s, 100/s** (EC2 để `CONSUMER_MODE=batch`).
2. **Sustained 1.000 sự kiện @ 100/s — Single-commit**: sửa `.env` trên EC2 thành `CONSUMER_MODE=single`, chạy `docker compose up -d consumer`, nhấn Enter.
3. **Sustained 1.000 sự kiện @ 100/s — Batch-commit (batch=100)**: sửa `.env` trên EC2 thành `CONSUMER_MODE=batch`, chạy `docker compose up -d consumer`, nhấn Enter.
4. **Crash Recovery (`exit code 77`)** *(dùng container one-shot để tránh `restart: unless-stopped` khởi động lại trong trạng thái vẫn bật crash)*:
   ```bash
   docker compose stop consumer
   docker compose run --rm -e CONSUMER_MODE=single -e CONSUMER_CRASH_AFTER_DB_COMMIT=true -e CONSUMER_CRASH_TARGET_INDEX=0 consumer
   ```
   Script gửi 100 sự kiện; container sẽ tự dừng ngay với **`exit code 77`** và log `INJECTED_CRASH_AFTER_DB_COMMIT`. Sau đó bật lại consumer bình thường (`batch` mode) để replay Kafka offset và log `DEDUP_REPLAY`:
   ```bash
   docker compose up -d consumer
   docker logs --tail 25 wq-consumer
   ```
   Nhấn Enter để `evaluate_run.py` xác nhận `duplicate_rows = 0` và `kafka_lag = 0`.
5. Script tự gọi `generate_charts.py` sinh 2 ảnh mới vào `benchmarks/charts/`:
   - `slide9_cloud_throughput.png`
   - `slide10_cloud_recovery.png`

### Kiểm chứng Bảo mật (cho Slide 11)

Chạy từ máy cá nhân (PowerShell) để chứng minh cổng Kafka đã bị chặn từ Internet trong khi SSH tunnel hoạt động:

```powershell
# Kết quả phải là TcpTestSucceeded : False (cổng 9092 và 19092 bị chặn ngoài Internet)
Test-NetConnection -ComputerName <EC2_IP> -Port 19092
Test-NetConnection -ComputerName <EC2_IP> -Port 9092
# Chỉ cổng 3000 (Grafana) và 22 (SSH) cho phép truy cập
Test-NetConnection -ComputerName <EC2_IP> -Port 3000
```

Trên EC2 kiểm tra port bind nội bộ:

```bash
ss -tlnp | grep -E "19092|3000"
# 19092 chỉ listen trên 127.0.0.1:19092 (không phải 0.0.0.0)
```

---

## Lưu ý tiết kiệm AWS Credit sau khi test xong

Khi chưa thuyết trình, nhớ **Stop EC2 instance** (đừng Terminate để giữ nguyên ổ đĩa/cấu hình; lưu ý khi Start lại IP Public có thể đổi trừ khi gắn Elastic IP):
- AWS Console → EC2 → Instance state → **Stop instance**.
- Neon Free Tier tự động suspend compute sau 5 phút không có truy vấn nên không tốn phí trong lúc chờ.
