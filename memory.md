# Project Memory & Quy tắc Bắt buộc (`water-quality-cloud`)

Tài liệu này là **bộ nhớ làm việc liên tục** của dự án **Water Quality Cloud Monitoring Pipeline** (Nhóm 12 — L01, môn Điện toán Đám mây — HCMUT). Mọi thành viên và AI agent làm việc trên repo này phải đọc trước khi chỉnh sửa và cập nhật lại sau mỗi cột mốc quan trọng.

> **Ghi chú bảo mật:** File này nằm trong Git tracked tree nên **tuyệt đối không chứa** địa chỉ IP EC2 thật, EC2 Instance ID, endpoint Neon thật, mật khẩu, hay đường dẫn khóa `.pem` cá nhân. Chi tiết hạ tầng live nằm trong file được Git bỏ qua: `docs/CLOUD_VERIFICATION_REPORT.md`.

---

## 1. Thông tin Dự án & Cấu trúc Hiện tại

- **Tên dự án**: Water Quality Cloud Monitoring Pipeline (`water-quality-cloud/`)
- **Lộ trình 2 pha**:
  - **Pha 1 (Seminar MVP)**: Thiết kế, triển khai, bảo mật và đo kiểm pipeline streaming thời gian thực trên **AWS EC2 (IaaS) + Neon Serverless PostgreSQL (Managed DB) + Grafana**.
  - **Pha 2 (Bài tập lớn — BTL 12/12/2026)**: Tối ưu chính pipeline theo **Nhóm 1 & Nhóm 2** trong `docs/BTL_CLOUD_NATIVE_ROADMAP.md` (SQL View theo trạm, đưa EC2 về cùng vùng `ap-southeast-1`, ghi bảng `pipeline_runs`, log dedup `rejected_events`, AWS Lambda cảnh báo `CRITICAL`, và FastAPI HTTPS Gateway).
- **Kiến trúc 3 phân vùng**:
  1. **Vùng 1 (Client Edge — Laptop)**: `producer/replay_producer.py` đọc CSV 2025 → JSON → gửi qua SSH Tunnel (`127.0.0.1:19092`).
  2. **Vùng 2 (AWS EC2 — Docker Compose)**: `wq-kafka` (KRaft 7.7.1, 3 partitions) + `wq-kafka-init` + `wq-consumer` (Python 3.12 non-root) + `wq-grafana` (Grafana 11.3.0, port `3000`).
  3. **Vùng 3 (Neon Cloud — Managed Serverless PostgreSQL 16)**: Pooled PgBouncer (`sslmode=require`), 3 bảng trong `db/schema.sql`.
- **Chế độ vận hành song song**:
  - **Offline (Local)**: `compose.yaml` + `compose.local.yaml` (thay Neon bằng container `wq-local-postgres` nội bộ).
  - **Online (Cloud)**: `compose.yaml` trên EC2 + Neon PostgreSQL + SSH Tunnel.
- **Bộ dữ liệu chính**:
  - Vị trí độc lập hoàn toàn trong repo: `water-quality-cloud/data/2025-C - Copy.csv` (`177.319` bản ghi thực tế UK Environment Agency năm 2025, `7.371` trạm, `483` chỉ tiêu) và `water-quality-cloud/data/2025-C_sample.csv` (`14` dòng mẫu cho unit test/fallback).

---

## 2. Những gì ĐÃ HOÀN THÀNH (Completed Log)

### 2.1. Hạ tầng, Container & Cấu hình (12-Factor)
- `compose.yaml`: Định nghĩa 4 service Cloud (`kafka` KRaft dual-listener `INTERNAL://kafka:9092` & `EXTERNAL://127.0.0.1:19092`, `kafka-init` tạo topic 3 partitions, `consumer`, `grafana`), mạng bridge `water_net`, volumes `kafka_data` & `grafana_data`.
- `compose.local.yaml`: Override chạy **Offline** hoàn toàn bằng `postgres:16-alpine` (`wq-local-postgres`), tự nạp `db/schema.sql`, cố ý không publish cổng `5432` ra host, override `DATABASE_URL` và `NEON_*` sang `postgres` nội bộ với `sslmode=disable`.
- `.env.example` & `.gitignore`: Chuẩn hóa biến môi trường 12-Factor; chặn commit `.env`, khóa `.pem/.key`, `*.csv`, `*.log`, JSON/PNG benchmark tự sinh, và file báo cáo chứa thông số live (`docs/CLOUD_VERIFICATION_REPORT.md`).

### 2.2. Cơ sở dữ liệu (`db/`)
- `db/schema.sql`: Tạo 3 bảng (`water_measurements` với `PRIMARY KEY (run_id, event_id)`, `rejected_events` với `PRIMARY KEY (run_id, event_id)`, và `pipeline_runs`) kèm 5 index tối ưu truy vấn Grafana.
- `db/init_db.py`: Khởi tạo schema idempotent trên Neon/PostgreSQL với `prepare_threshold=None` và xác nhận `3/3` bảng.

### 2.3. Pipeline Producer & Consumer (`producer/`, `consumer/`)
- `producer/replay_producer.py`: Đọc CSV, lọc mặc định `CORE_DETERMINANDS` (8 nhãn gồm 7 chỉ tiêu chuẩn + `NO FLOW/SAMP`), gắn partition key = `station_id`, gán `produced_at` UTC, điều tiết tốc độ `--rate`, hỗ trợ `--loop` gắn hậu tố `-loopN`, flush an toàn khi `SIGINT`/`SIGTERM`, xuất JSON summary.
- `consumer/app/validator.py`: Kiểm định schema, lọc `NON_MEASUREMENT_CODED` (`NO FLOW/SAMP` hoặc `unit == "coded"`), kiểm tra giá trị số hữu hạn/âm/miền vật lý (`pH` `[0,14]`, `Temp Water` `[-5,60]`), phân loại `alert_level` (`NORMAL`, `WARNING`, `CRITICAL`) theo bảng ngưỡng `THRESHOLDS` cho 7 chỉ tiêu.
- `consumer/app/db.py`: Ghi batch nguyên tử cả `water_measurements` và `rejected_events` trong 1 transaction bằng `UNNEST ... ON CONFLICT (run_id, event_id) DO NOTHING RETURNING event_id`, đặt `prepare_threshold=None` cho PgBouncer, retry exponential backoff (`1s..8s`) khi Neon cold-start (`OperationalError`).
- `consumer/app/main.py`: Manual Kafka offset commit (`enable.auto.commit=False`), chỉ commit offset **sau khi** DB commit thành công; hỗ trợ `CONSUMER_MODE=batch|single`, dual-trigger flush (kích thước batch hoặc timeout), log JSON chuẩn (`BATCH_COMMITTED`, `DEDUP_REPLAY`, `INJECTED_CRASH_AFTER_DB_COMMIT`), và hook test crash (`exit code 77`).
- `consumer/Dockerfile`: Image `python:3.12-slim` chạy dưới quyền user non-root `appuser`.

### 2.4. Grafana Provisioning (`grafana/provisioning/`)
- `datasources/neon-postgres.yaml`: Tự động cấu hình datasource PostgreSQL (`uid: neon-postgres`) qua biến môi trường (`NEON_HOST`, `NEON_DB`, `NEON_USER`, `NEON_PASSWORD`, `PG_SSLMODE`).
- `dashboards/dashboards.yaml` & `dashboards/json/water-quality-monitoring.json`: Dashboard `Water Quality Cloud Monitoring (N12-L01)` (`uid: wq-cloud-n12-l01`, auto-refresh `5s`) gồm **10 panels** và **2 biến lọc** (`$run_id`, `$determinand`).

### 2.5. Công cụ Benchmark, Biểu đồ & Thực nghiệm thật trên Cloud
- `benchmarks/evaluate_run.py`, `benchmarks/run_seminar_benchmarks.ps1`, `benchmarks/generate_charts.py`, `benchmarks/generate_canva_assets.py`.
- **Kết quả kiểm chứng thật trên Cloud ngày 23/11/2026** (AWS EC2 `ap-southeast-2` → Neon PostgreSQL `ap-southeast-1`, xem chi tiết trong `docs/CLOUD_VERIFICATION_REPORT.md`):
  1. **Burst 300 sự kiện @ 10 / 50 / 100 evt/s** (`batch=100`): Producer đạt `9.81`, `47.10`, `88.45 evt/s`; DB ghi tới `202.35 evt/s`, `239` hợp lệ + `61` `NO FLOW/SAMP` bị lọc, `0` trùng, `kafka_lag = 0`.
  2. **Sustained 1.000 sự kiện @ 100 evt/s — Single vs Batch qua WAN**:
     - `single` (`batch=1`): mất `332.487 s`, đạt **`3.01 evt/s`**, E2E `p95 = 316.98 s` (`max = 328.41 s`).
     - `batch=100` (`UNNEST`): mất `6.638 s`, đạt **`150.65 evt/s`** (nhanh gấp **~50 lần**), E2E `p95 = 4.47 s` (giảm **~71 lần**).
  3. **Crash Recovery (`exit code 77`)**: Tiêm crash sau commit DB nhưng trước commit Kafka offset → Consumer restart replay toàn bộ 100 bản ghi → `ON CONFLICT DO NOTHING` loại trùng → `74` hợp lệ + `26` bị loại = `100`, `duplicate_rows = 0`, `kafka_lag = 0`.
  4. **Bảo mật & Grafana**: Port `9092` & `19092` bị chặn từ Internet (`TcpTestSucceeded: False`), port `3000` mở có kiểm soát; 10/10 panel Grafana hiển thị ổn định.

### 2.6. Bộ kiểm thử tự động (`tests/`)
- `tests/test_validator.py` & `tests/test_pipeline_logic.py`: Kiểm tra validate CSV thật, lọc `NO FLOW/SAMP`, miền vật lý, ngưỡng cảnh báo, chuẩn hóa UTC, tạo `event_id`, cơ chế `UNNEST` dedup counting, `compute_run_metrics`, sinh ảnh smoke test, và ưu tiên đọc CSV trong `data/`.

### 2.7. Cập nhật ngày 24/11/2026
- Tạo `water-quality-cloud/data/` kèm `.gitkeep`; **sao chép** (giữ nguyên bản gốc) `2025-C - Copy.csv` và `2025-C_sample.csv` từ `../Đánh giá mô hình/` vào `water-quality-cloud/data/`.
- Cập nhật `default_csv_path()` trong `producer/replay_producer.py` và `tests/test_validator.py` để ưu tiên đọc CSV từ `data/` trước, giữ fallback sang `../Đánh giá mô hình/`. Thêm unit test `test_default_csv_path_prefers_project_data_dir()`.
- Tạo 2 tài liệu chuyên sâu: `docs/OFFLINE_ONLINE_STEP_BY_STEP_GUIDE.md` (hướng dẫn từng bước chạy Offline & Online) và `docs/OFFLINE_ONLINE_EXECUTION_FLOW.md` (giải thích luồng hoạt động Offline & Online).
- Tạo file `memory.md` và cập nhật `README.md`.

### 2.8. Cập nhật ngày 25/11/2026
- Rà soát và đồng bộ toàn bộ 7 tài liệu trong `docs/` với mã nguồn hiện tại (`compose.yaml`, `replay_producer.py`, `consumer/app/*`, `benchmarks/*`, `grafana/provisioning/*`, `tests/`):
  - Đồng bộ cấu hình EC2 (`c7i-flex.large` / `t3.medium`, `ap-southeast-2`), lệnh `scp` chọn lọc tránh copy 62 MB CSV & `.venv`, link Grafana đầy đủ `/d/wq-cloud-n12-l01`, kịch bản demo & script `generate_canva_assets.py` trong `DEPLOY_EC2_NEON_GUIDE.md` và `SEMINAR_ACTION_PLAN_AND_SLIDES.md`.
  - Đính chính số liệu kiểm chứng ML từ script `online-model.py` (`1.501/1.517` trạm có `R² < 0`) trong `BTL_CLOUD_NATIVE_ROADMAP.md`.
  - Bổ sung ghi chú rõ ràng về điều kiện in log `DEDUP_REPLAY` (`deduped_valid > 0`) vs `rejected_events`, `mkdir -p ~/water-quality-cloud` trước `scp`, và phạm vi `$__timeFilter(ingested_at)` của Grafana trong `OFFLINE_ONLINE_STEP_BY_STEP_GUIDE.md` và `OFFLINE_ONLINE_EXECUTION_FLOW.md`.
  - Đồng bộ đường dẫn `data/2025-C - Copy.csv`, số lượng `11` unit tests, bảng bản đồ file và ghi chú lượt benchmark bổ sung `16:57–16:58 UTC` ngày `23/11/2026` trong 2 file gitignored `SEMINAR_ARCHITECTURE_AND_WORKFLOW_GUIDE.md` và `CLOUD_VERIFICATION_REPORT.md`.

### 2.9. Cập nhật ngày 26/11/2026 (Tài liệu nhập môn & Clean Repo trên nhánh `chore/clean-repo-and-docs`)
- Tạo tài liệu `docs/BEGINNER_SYSTEM_AND_SLIDE_GUIDE.md` giải thích toàn bộ hệ thống cho người mới (ẩn dụ đời thực, hành trình 2 bản ghi thật, vai trò 8 công cụ, 7 câu hỏi "Tại sao?" gắn với 5 ADR) và bảng đối chiếu chi tiết 19 slide Canva (`DAHbuYdPunA`) với source code hiện tại.
- Tạo nhánh `chore/clean-repo-and-docs` và dọn sạch repo:
  - Gỡ bảng thừa `ml_predictions` khỏi `db/schema.sql`, `db/init_db.py` và chạy `DROP TABLE IF EXISTS ml_predictions` trên Neon DB thật (`3/3` bảng).
  - Gỡ hoàn toàn tham chiếu thư mục cũ trong `producer/replay_producer.py` & `tests/test_validator.py` để `water-quality-cloud/` độc lập 100%; bổ sung `test_schema_and_init_db_expect_three_tables()` (`12/12` pytest PASS); nâng `-WaitTimeout` của `sustained-single` trong `benchmarks/run_seminar_benchmarks.ps1` từ `300` lên `480` giây.
  - Xóa 3 tài liệu trùng/cũ (`DEPLOY_EC2_NEON_GUIDE.md`, `SEMINAR_ACTION_PLAN_AND_SLIDES.md`, `SEMINAR_ARCHITECTURE_AND_WORKFLOW_GUIDE.md`), giữ gọn `5` tài liệu chuẩn trong `docs/`, dọn `.gitignore`, và xóa `7` file JSON chạy dở (`23/11` tối) trong `benchmarks/results/` (giữ 2 bộ đầy đủ `23/11` và `25/11`).
  - Viết lại `docs/BTL_CLOUD_NATIVE_ROADMAP.md` chỉ giữ phạm vi **Nhóm 1 & Nhóm 2** (bỏ Nhóm 3: `k3s + KEDA`, `Terraform + SSM`, `Prometheus/cAdvisor`, `ML Worker`), đồng bộ các docs còn lại, `README.md` và `memory.md`.

---

## 3. Những việc CÒN LẠI / Lộ trình Tiếp theo

### 3.1. Hoàn thiện Seminar (Ngắn hạn)
- Chỉnh 5 điểm nhỏ trên bộ slide Canva `DAHbuYdPunA` theo checklist ở Mục 5.2 của `docs/BEGINNER_SYSTEM_AND_SLIDE_GUIDE.md` (`Slide 2, 5, 11, 17, 18`).
- Quay video dự phòng 2 phút (Full HD) chạy demo live phòng mạng hội trường chập chờn.
- Tổng duyệt bấm giờ (< 15 phút) và luyện tập vấn đáp.

### 3.2. Giai đoạn Bài tập lớn (Hạn 12/12 — chỉ làm Nhóm 1 & Nhóm 2 theo `docs/BTL_CLOUD_NATIVE_ROADMAP.md`)
- **Nhóm 1 (Ưu tiên cao — bắt buộc):**
  1. Tạo SQL View `v_station_water_summary` tổng hợp chất lượng nước theo trạm trong `db/schema.sql` & hiển thị trên Grafana.
  2. Dựng EC2 về cùng vùng `ap-southeast-1` (Singapore) với Neon DB để đo A/B (*Sydney* vs *Singapore*) và khảo sát nhanh `batch_size`.
  3. Tự động ghi metadata mỗi phiên chạy vào bảng `pipeline_runs`, và bổ sung log `DEDUP_REPLAY` cho cả `rejected_events`.
  4. Tạo hàm **AWS Lambda (`$0`)** bắn cảnh báo Telegram/Discord ngay khi phát hiện bản ghi `CRITICAL`.
- **Nhóm 2 (Làm tiếp nếu đủ thời gian):**
  5. Bổ sung **FastAPI HTTPS Ingestion Gateway** (`POST /events` + `X-API-Key` sau Caddy HTTPS) thay thế SSH Tunnel cho nhiều client.

---

## 4. NHỮNG QUY TẮC BẮT BUỘC LÀM THEO (Mandatory Rules)

### A. Bảo mật & Git Hygiene
1. **KHÔNG BAO GIỜ commit bí mật lên Git**: Tuyệt đối không đưa `.env`, khóa SSH (`.pem`, `.key`, `.ppk`), mật khẩu Neon/Grafana, hoặc token AWS vào bất kỳ file nào được Git theo dõi.
2. **KHÔNG BAO GIỜ ghi thông tin hạ tầng nhạy cảm vào file tracked**: Không ghi Public IP của EC2, EC2 Instance ID, endpoint host Neon thật, hoặc đường dẫn username cá nhân trên máy Windows vào `README.md`, `memory.md`, hay các tài liệu hướng dẫn chung — luôn dùng placeholder (`<EC2_IP>`, `<NEON_HOST>`, `<PATH_TO_PEM>`).
3. **KHÔNG commit file CSV nặng và artifact benchmark sinh tự động**: Giữ nguyên luật `.gitignore` chặn `*.csv`, `*.log`, `benchmarks/results/*.json`, `benchmarks/charts/*.png`. Thư mục `data/` chỉ track duy nhất `.gitkeep`.
4. **KHÔNG mở cổng Kafka ra Internet**: Trong `compose.yaml`, cổng `19092` luôn bind `127.0.0.1:19092:19092` (không phải `0.0.0.0`); AWS Security Group chỉ được mở TCP `22` (My IP) và TCP `3000` (My IP / phòng học lúc demo), đóng tuyệt đối `9092` và `19092`.
5. **KHÔNG mở cổng PostgreSQL Offline ra máy host**: Giữ `compose.local.yaml` không publish cổng `5432`; mọi truy vấn thủ công chạy qua `docker exec -i wq-local-postgres psql ...`.
6. **Bắt buộc TLS & Non-root**: Kết nối Neon luôn đặt `sslmode=require` (`PG_SSLMODE=require`); container Consumer giữ chạy bằng user non-root `appuser`.

### B. Kiến trúc & Tính đúng đắn Dữ liệu
7. **Thứ tự Commit bất biến (At-Least-Once)**: Luôn giữ `enable.auto.commit=False` trên Kafka Consumer. Bắt buộc `conn.commit()` xuống PostgreSQL/Neon **TRƯỚC**, rồi mới gọi `consumer.commit(asynchronous=False)`. Không bao giờ đảo ngược thứ tự này.
8. **Chống trùng bằng Idempotent Write**: Giữ nguyên `PRIMARY KEY (run_id, event_id)` trên `water_measurements`, `rejected_events` và câu lệnh `UNNEST ... ON CONFLICT (run_id, event_id) DO NOTHING RETURNING event_id`.
9. **Tương thích PgBouncer của Neon**: Mọi kết nối `psycopg.connect(...)` tới Neon (trong `db.py`, `init_db.py`, `evaluate_run.py`) bắt buộc giữ `prepare_threshold=None`.
10. **Giữ Partition Key theo Trạm**: Producer luôn dùng `station_id` làm Kafka key để bảo toàn thứ tự thời gian từng trạm và sẵn sàng mở rộng nhiều Consumer theo partition.
11. **Bảo toàn 3 mốc thời gian**: Không trộn lẫn `event_time` (thời điểm mẫu 2025 trong CSV), `produced_at` (thời điểm Producer gửi UTC), và `ingested_at` (`clock_timestamp()` của DB). End-to-End Latency luôn tính bằng `ingested_at - produced_at`.
12. **12-Factor Config & 1 Codebase duy nhất**: Mọi cấu hình runtime đọc từ biến môi trường. Không viết nhánh code `if offline / else online` trong `producer/` hay `consumer/`; mọi khác biệt Offline/Online chỉ nằm ở `compose.local.yaml` và `.env`.

### C. Vận hành & Chi phí Cloud
13. **Tránh xung đột cổng `127.0.0.1:19092`**: Tuyệt đối không chạy đồng thời Kafka Offline (`compose.local.yaml`) và SSH Tunnel Online trên cùng máy. Tắt dứt điểm bên cũ trước khi bật bên kia.
14. **An toàn khi tiêm lỗi Crash (`exit code 77`)**: Trong `.env` luôn giữ `CONSUMER_CRASH_AFTER_DB_COMMIT=false`. Khi test crash recovery, bắt buộc `docker compose stop consumer` và chạy container dùng một lần `docker compose run --rm -e CONSUMER_CRASH_AFTER_DB_COMMIT=true ... consumer` (tránh `restart: unless-stopped` khiến container crash lặp vô tận).
15. **Tiết kiệm AWS Credit**: Sau khi demo/benchmark xong, luôn chọn **Stop instance** trên AWS Console (tuyệt đối **không Terminate** nếu chưa muốn xóa hạ tầng). Khi Start lại, nhớ kiểm tra Public IPv4 mới và cập nhật **My IP** trong Security Group nếu đổi mạng Wi-Fi.

### D. Trung thực Khoa học, Dữ liệu & Kiểm thử
16. **Trung thực 100% số liệu & thuật ngữ**:
    - Không bịa đặt số benchmark hay chi phí AWS/Neon; mọi con số báo cáo phải trích trực tiếp từ JSON kết quả thực nghiệm hoặc trang giá chính thức kèm ngày tra.
    - Không dùng kết quả chạy Offline (Docker localhost) để trình bày thay cho số liệu Cloud WAN.
    - Trình bày chính xác cơ chế hệ thống là **At-Least-Once Delivery + Idempotent Write** (không nói quá thành distributed 2PC exactly-once).
17. **Độc lập thư mục dữ liệu**: Mọi script streaming và kiểm thử trong `water-quality-cloud/` chỉ đọc CSV từ `water-quality-cloud/data/`; không phụ thuộc ngược vào thư mục bên ngoài repo.
18. **Bắt buộc chạy kiểm thử trước khi commit hoặc deploy**: Chạy `.\.venv\Scripts\pytest tests -v` và xác nhận `100% PASSED` trước mỗi lần đẩy code hoặc cập nhật lên EC2.
19. **Cập nhật bộ nhớ dự án**: Sau mỗi thay đổi kiến trúc, sửa bug quan trọng hoặc hoàn thành cột mốc mới, phải cập nhật lại `memory.md`.
