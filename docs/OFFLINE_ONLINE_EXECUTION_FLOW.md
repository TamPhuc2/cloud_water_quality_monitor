# Giải thích Luồng Hoạt động Chi tiết khi Chạy OFFLINE & ONLINE

Tài liệu này mô tả chi tiết **luồng điều khiển, luồng mạng và luồng dữ liệu** từ khi khởi động hạ tầng đến lúc bản ghi xuất hiện trên Grafana cho cả hai chế độ **OFFLINE** (Docker nội bộ) và **ONLINE** (AWS EC2 + Neon Cloud + SSH Tunnel).

> **Lưu ý bảo mật:** Mọi địa chỉ IP công cộng, tên endpoint Neon và đường dẫn khóa SSH trong tài liệu này dùng ký hiệu thay thế (`<EC2_IP>`, `<NEON_HOST>`, `<PATH_TO_PEM>`).

---

## 1. Nguyên lý thiết kế: "1 Codebase – 2 Chế độ" (12-Factor & Compose Override)

Toàn bộ mã nguồn nghiệp vụ của hệ thống là **duy nhất** cho cả Offline và Online:
- `producer/replay_producer.py` luôn đọc CSV và đẩy sự kiện vào `KAFKA_BOOTSTRAP_SERVERS` (mặc định `127.0.0.1:19092`).
- `consumer/app/` (`main.py`, `validator.py`, `db.py`) luôn đọc cấu hình từ biến môi trường (`KAFKA_BOOTSTRAP_SERVERS`, `DATABASE_URL`, `CONSUMER_MODE`, `CONSUMER_BATCH_SIZE`,...) — **không có nhánh `if offline / else online` trong code Python**.
- `db/schema.sql` và `grafana/provisioning/` dùng chung 100% giữa hai môi trường.

Sự khác biệt giữa Offline và Online được quyết định **hoàn toàn ở lớp hạ tầng & cấu hình Docker Compose**:

| Khía cạnh | Chế độ OFFLINE (`compose.yaml` + `compose.local.yaml`) | Chế độ ONLINE (`compose.yaml` trên AWS EC2) |
| :--- | :--- | :--- |
| **1. Điểm kết nối Producer** | `127.0.0.1:19092` (Docker Desktop map trực tiếp vào `wq-kafka`) | `127.0.0.1:19092` (SSH Tunnel port-forward sang `127.0.0.1:19092` trên EC2) |
| **2. Bảo mật kênh Producer → Kafka** | Loopback cục bộ trên cùng máy cá nhân (`127.0.0.1`) | Bọc toàn bộ trong **SSH Tunnel (Port 22)** mã hóa; cổng `9092/19092` bị Security Group chặn khỏi Internet |
| **3. Nơi chạy Kafka & Consumer** | Docker Desktop trên Laptop (`water_net`) | Máy ảo **AWS EC2** (`c7i-flex.large` / `t3.medium`, Ubuntu 24.04, `water_net`) |
| **4. Độ trễ mạng Consumer → DB** | `< 1 ms` qua cầu nối mạng Docker nội bộ | Độ trễ **WAN thực tế** xuyên Internet (EC2 ↔ Neon Cloud), phơi bày nút thắt RTT khi single-commit |
| **5. Database Engine** | `postgres:16-alpine` trong container `wq-local-postgres` | **Neon Serverless PostgreSQL 16** (Managed DB sau PgBouncer `-pooler`) |
| **6. TLS & Connection Pooling** | `sslmode=disable`, `PG_SSLMODE=disable` | Bắt buộc `sslmode=require` (TLS 5432) + `prepare_threshold=None` cho PgBouncer |
| **7. Khởi tạo Schema (`3` bảng)** | Tự động chạy `db/schema.sql` qua `/docker-entrypoint-initdb.d/01-schema.sql` khi volume trống | Chạy chủ động bằng `python db/init_db.py` trước khi dựng stack |
| **8. Grafana DataSource** | `compose.local.yaml` override `NEON_HOST=postgres`, `PG_SSLMODE=disable` | Dùng biến `.env` trỏ thẳng sang `<NEON_HOST>:5432` với `PG_SSLMODE=require` |
| **9. Truy vấn đánh giá / SQL** | `docker exec -i wq-local-postgres psql ...` (vì cổng `5432` không mở ra host) | `benchmarks/evaluate_run.py` nối tới Neon DB + Kafka lag qua SSH Tunnel |
| **10. Mục đích sử dụng** | Phát triển, debug, test logic & crash recovery miễn phí | Demo live trên lớp và đo kiểm số liệu Cloud thật |

---

## 2. Luồng hoạt động chi tiết khi chạy OFFLINE (Local Stack)

### 2.1. Sơ đồ kiến trúc thực thi Offline

```mermaid
flowchart LR
    subgraph HOST["Máy cá nhân (Windows / Local Host)"]
        CSV["data/2025-C - Copy.csv\n(177.319 dòng)"] --> PROD["replay_producer.py\n(key = station_id)"]
        BROWSER["Trình duyệt\nhttp://localhost:3000"]
    end

    subgraph DOCKER["Docker Engine — Mạng nội bộ water_net"]
        KAFKA["wq-kafka (KRaft 7.7.1)\nEXTERNAL: 127.0.0.1:19092\nINTERNAL: kafka:9092"]
        KINIT["wq-kafka-init\n(Tạo topic 3 partitions, thoát 0)"]
        CONS["wq-consumer (Python 3.12 non-root)\nValidate + Alert + Batch UNNEST"]
        PG[("wq-local-postgres\npostgres:16-alpine\nPort 5432 nội bộ (không publish host)")]
        GRAF["wq-grafana (11.3.0)\nPort host 3000:3000"]

        KINIT -.->|"Khởi tạo topic"| KAFKA
        KAFKA -->|"kafka:9092"| CONS
        CONS -->|"postgres:5432\nsslmode=disable"| PG
        GRAF -->|"SQL Query\npostgres:5432"| PG
    end

    subgraph VOLS["Docker Volumes (Lưu bền vững trên đĩa)"]
        VK["kafka_data"]
        VP["local_pg_data"]
        VG["grafana_data"]
    end

    PROD -->|"TCP 127.0.0.1:19092"| KAFKA
    BROWSER -->|"HTTP :3000"| GRAF
    KAFKA --- VK
    PG --- VP
    GRAF --- VG
```

### 2.2. Cơ chế ghi đè (Override) của `compose.local.yaml`

Khi chạy `docker compose -f compose.yaml -f compose.local.yaml up -d --build`, Docker Compose hợp nhất hai file cấu hình theo thứ tự ưu tiên **file sau đè file trước**:
1. **Thêm service `postgres` (`wq-local-postgres`)**:
   - Mount `./db/schema.sql:/docker-entrypoint-initdb.d/01-schema.sql:ro` để PostgreSQL tự tạo 3 bảng ngay lần đầu khởi tạo volume `local_pg_data`.
   - Cố ý **không khai báo `ports:`** để cổng `5432` không bị lộ ra máy host hay mạng LAN.
   - Gắn `healthcheck` dùng `pg_isready`.
2. **Ghi đè `consumer`**:
   - Bổ sung điều kiện `depends_on` chờ cả `kafka-init` (`service_completed_successfully`) **và** `postgres` (`service_healthy`).
   - Ghi đè `DATABASE_URL` thành `postgresql://water_user:water_local_pass@postgres:5432/water_db?sslmode=disable` (bỏ qua giá trị Neon trong `.env`).
3. **Ghi đè `grafana`**:
   - Chờ `postgres` đạt `service_healthy`.
   - Ghi đè `NEON_HOST=postgres`, `NEON_DB=water_db`, `NEON_USER=water_user`, `NEON_PASSWORD=water_local_pass`, `PG_SSLMODE=disable` để template `neon-postgres.yaml` kết nối trơn tru vào PostgreSQL container.

### 2.3. Chuỗi khởi động & vòng đời dữ liệu Offline

```mermaid
sequenceDiagram
    autonumber
    participant DC as Docker Compose
    participant PG as wq-local-postgres
    participant K as wq-kafka
    participant KI as wq-kafka-init
    participant C as wq-consumer
    participant G as wq-grafana
    participant P as replay_producer.py

    DC->>PG: Start postgres:16-alpine
    PG->>PG: Volume trống -> chạy 01-schema.sql tạo 3 bảng
    PG-->>DC: Healthcheck pg_isready = healthy
    DC->>K: Start Kafka KRaft
    K-->>DC: Healthcheck broker-api-versions = healthy
    DC->>KI: Start one-shot kafka-init
    KI->>K: Tạo topic water-quality-events (3 partitions)
    KI-->>DC: Exit 0 (completed_successfully)
    DC->>C: Start consumer (sau kafka-init + postgres healthy)
    DC->>G: Start grafana (sau postgres healthy)
    C->>K: Subscribe topic water-quality-events (enable.auto.commit=False)

    P->>P: Đọc data/2025-C - Copy.csv, lọc CORE_DETERMINANDS
    P->>K: Gửi JSON qua 127.0.0.1:19092 (key=station_id, acks=all)
    K-->>P: Delivery callback ACK
    C->>K: poll() gom tối đa 100 bản tin hoặc 500ms
    C->>C: validate_event() & classify_alert()
    C->>PG: 1 transaction UNNEST ON CONFLICT DO NOTHING (mạng nội bộ < 1ms)
    PG-->>C: Commit thành công
    C->>K: consumer.commit(asynchronous=False)
    G->>PG: Truy vấn SQL mỗi 5s hiển thị tại localhost:3000
```

> **Lưu ý:** Trong môi trường Offline, vòng mạng giữa `wq-consumer` và `wq-local-postgres` đi hoàn toàn trong RAM/kernel Linux của Docker (`< 1 ms`), nên độ trễ `single-commit` và `batch-commit` sẽ chênh lệch rất nhỏ — không dùng số đo Offline để thay cho minh chứng nút thắt WAN trên Cloud.

---

## 3. Luồng hoạt động chi tiết khi chạy ONLINE (AWS EC2 + Neon Cloud)

### 3.1. Sơ đồ kiến trúc thực thi Online (3 Phân vùng)

```mermaid
flowchart LR
    subgraph Z1["Vùng 1: Client Edge (Laptop cá nhân)"]
        CSV["data/2025-C - Copy.csv\n(177.319 bản ghi)"] --> PROD["replay_producer.py\n(key = station_id)"]
        EVAL["benchmarks/evaluate_run.py"]
        USER["Trình duyệt\nhttp://<EC2_IP>:3000"]
    end

    subgraph SEC["Kênh truyền mã hóa qua Internet"]
        SSH["SSH Tunnel (EC2 Port 22)\n127.0.0.1:19092 -> EC2 127.0.0.1:19092\n(Port 9092 & 19092 khóa trên Security Group)"]
    end

    subgraph Z2["Vùng 2: AWS EC2 (Docker Compose, water_net)"]
        KAFKA["wq-kafka (KRaft 7.7.1)\nEXTERNAL: 127.0.0.1:19092\nINTERNAL: kafka:9092"]
        KINIT["wq-kafka-init\n(3 partitions)"]
        CONS["wq-consumer\nValidate + Alert + Batch UNNEST"]
        GRAF["wq-grafana (11.3.0)\nPort 3000 (Security Group My IP)"]
        KINIT -.-> KAFKA
        KAFKA -->|"kafka:9092"| CONS
    end

    subgraph Z3["Vùng 3: Neon Cloud (Managed Serverless PostgreSQL 16)"]
        PGB["PgBouncer Pooler\n(<NEON_HOST>:5432, sslmode=require)"]
        NEON[("Neon DB\nwater_measurements\nrejected_events\npipeline_runs")]
        PGB --> NEON
    end

    PROD -->|"127.0.0.1:19092"| SSH -->|"Loopback trên EC2"| KAFKA
    CONS -->|"Internet WAN + TLS 5432\nUNNEST ON CONFLICT DO NOTHING"| PGB
    GRAF -->|"Internet WAN + TLS 5432"| PGB
    EVAL -->|"SQL TLS 5432"| PGB
    EVAL -.->|"Kafka Lag qua SSH Tunnel"| SSH
    USER -->|"HTTP :3000"| GRAF
```

### 3.2. Thiết kế Dual-Listener của Kafka & cơ chế SSH Tunnel

Một điểm thiết kế then chốt trong `compose.yaml` là khai báo **hai listener tách biệt** trên `wq-kafka`:
- **`INTERNAL://kafka:9092`** (`KAFKA_ADVERTISED_LISTENERS` = `INTERNAL://kafka:9092`): dành riêng cho `wq-kafka-init` và `wq-consumer` giao tiếp bên trong mạng Docker `water_net`.
- **`EXTERNAL://127.0.0.1:19092`** (`ports: - "127.0.0.1:19092:19092"`): bind **chỉ vào loopback `127.0.0.1` của máy chủ EC2**, không bao giờ bind `0.0.0.0`, khiến Internet không thể kết nối trực tiếp vào Kafka dù lỡ mở nhầm Security Group.

**Điều gì xảy ra khi Laptop gửi bản tin qua SSH Tunnel?**
1. Khi chạy `ssh -i "<PATH_TO_PEM>" -N -L 19092:127.0.0.1:19092 ubuntu@<EC2_IP>`, SSH Client mở cổng `127.0.0.1:19092` trên Laptop và chuyển tiếp mọi gói TCP qua kênh mã hóa SSH (cổng `22`) sang `127.0.0.1:19092` trên EC2.
2. `replay_producer.py` hỏi metadata từ `127.0.0.1:19092`. Kafka trả về metadata advertised listener là **`127.0.0.1:19092`** — địa chỉ này khớp chính xác với đầu vào SSH Tunnel đang mở trên Laptop!
3. Nhờ vậy, Producer giữ nguyên một địa chỉ `--bootstrap 127.0.0.1:19092` duy nhất cho cả Offline lẫn Online mà không cần sửa code hay đổi cấu hình DNS.

### 3.3. Chuỗi hoạt động End-to-End Online & Tác động của độ trễ WAN

```mermaid
sequenceDiagram
    autonumber
    participant L as Laptop (Edge)
    participant T as SSH Tunnel (Port 22)
    participant K as EC2: wq-kafka
    participant C as EC2: wq-consumer
    participant N as Neon PgBouncer (TLS 5432)
    participant G as EC2: wq-grafana

    Note over L,N: Tiền điều kiện: python db/init_db.py tạo sẵn 3 bảng trên Neon
    L->>T: Mở tunnel 127.0.0.1:19092 -> EC2 127.0.0.1:19092
    L->>T: replay_producer.py gửi JSON (produced_at = UTC)
    T->>K: Lưu bền vững xuống EBS volume kafka_data (3 partitions)
    C->>K: consumer.poll() nội bộ qua kafka:9092
    C->>C: validate_event() & phân loại NORMAL/WARNING/CRITICAL
    C->>N: Kết nối TLS (prepare_threshold=None), 1 vòng mạng UNNEST
    alt Neon đang auto-suspend
        N-->>C: OperationalError (cold start)
        C->>C: Backoff 1s -> 2s -> 4s -> 8s rồi reconnect
    end
    N-->>C: COMMIT xong (clock_timestamp() -> ingested_at)
    C->>K: consumer.commit(asynchronous=False)
    G->>N: Truy vấn SQL qua TLS 5432 mỗi 5s
    L->>G: Trình duyệt xem dashboard tại http://<EC2_IP>:3000
```

**Vì sao chế độ Online bắt buộc phải chạy `batch` (`UNNEST`)?**
- Ở chế độ `CONSUMER_MODE=single` (`batch_size=1`), mỗi 1 bản ghi tốn 1 vòng RTT mạng WAN + TLS qua biển từ EC2 tới Neon + 1 lần `COMMIT` đồng bộ + 1 lần commit offset Kafka. Khi đo thực nghiệm thật trên AWS EC2 (`ap-southeast-2`) ↔ Neon (`ap-southeast-1`) ngày 23/11/2026 với `1.000` sự kiện @ `100 evt/s`:
  - **`single`**: mất `332.487 s`, thông lượng DB chỉ đạt **`3.01 evt/s`**, độ trễ E2E `p95 = 316.98 s` (`max = 328.41 s`). Trong suốt hơn 5 phút đó, `wq-kafka` đóng vai trò vùng đệm giữ an toàn toàn bộ backlog không rơi bản ghi nào.
  - **`batch=100` (`UNNEST`)**: gom 100 dòng vào **1 câu SQL mảng duy nhất**, giảm số round-trip WAN đi 100 lần nên ghi xong chỉ trong `6.638 s` (`150.65 evt/s` — **nhanh hơn ~50 lần**), `p95 = 4.47 s` (**giảm ~71 lần**).

---

## 4. Luồng xử lý dữ liệu bên trong 6 chặng cốt lõi (Chung cho Offline & Online)

### Chặng 1 — Đọc CSV, Lọc & Điều tiết tốc độ (`producer/replay_producer.py`)

1. **Tìm nguồn CSV (`default_csv_path`)**: Đọc từ `water-quality-cloud/data/2025-C - Copy.csv` (`177.319` dòng), fallback sang `water-quality-cloud/data/2025-C_sample.csv`.
2. **Lọc chỉ tiêu (`_iter_filtered_rows`)**: Mặc định `--core-only=True`, giữ 8 nhãn trong `CORE_DETERMINANDS`:
   - 7 chỉ tiêu đo lường: `BOD ATU`, `Sld Sus@105C`, `Ammonia(N)`, `COD as O2`, `Phosphorus-P`, `pH`, `Temp Water`.
   - 1 nhãn nhiễu có chủ đích: `NO FLOW/SAMP` (để kiểm chứng bộ lọc của Consumer ở chặng 3).
   - *(Nếu truyền `--all-determinands`, Producer phát toàn bộ 483 chỉ tiêu trong CSV.)*
3. **Đóng gói JSON & Định danh chống trùng (`csv_row_to_event`)**:
   - `event_id`: cắt phần đuôi của cột `@id` (ví dụ `AN-2089657-0085`). Nếu bật `--loop`, từ vòng lặp thứ 2 trở đi gắn thêm hậu tố `-loop1`, `-loop2`,... để giữ khóa chính `(run_id, event_id)` luôn duy nhất khi demo liên tục.
   - `station_id`: lấy từ `sample.samplingPoint.notation`, dùng làm **Kafka Partition Key**.
   - `event_time`: chuẩn hóa timestamp naive 2025 thành UTC (`...Z`).
   - `produced_at`: thời điểm UTC hiện tại ngay lúc Producer tạo sự kiện.
4. **Phát hành an toàn & điều tiết tốc độ**:
   - Dùng `time.monotonic()` tính `target_t = start_mono + (sent / rate)` để giữ nhịp chính xác (`10`, `20`, `50`, `100 evt/s`, hoặc `0` để bắn burst).
   - Cấu hình `confluent_kafka.Producer`: `acks="all"`, `linger.ms=5`, `compression.type="lz4"`, `retries=5`.
   - Nếu hàng đợi nội bộ của `librdkafka` đầy (`BufferError`), gọi `producer.poll(0.1)` giải phóng callback rồi gửi tiếp; khi nhấn `Ctrl+C` (`SIGINT`/`SIGTERM`), khối `finally` luôn gọi `producer.flush(timeout=30.0)` để không làm rơi gói đang gửi dở.

### Chặng 2 — Hàng đợi sự kiện phân vùng (`wq-kafka` & `wq-kafka-init`)

- Broker chạy chế độ **KRaft** (`broker,controller`), bỏ hoàn toàn Zookeeper để tiết kiệm RAM.
- Tắt tự động tạo topic (`KAFKA_AUTO_CREATE_TOPICS_ENABLE="false"`); thay vào đó `wq-kafka-init` tạo tường minh topic `water-quality-events` với **`3 partitions`** (`replication-factor 1`).
- Nhờ **Partition Key = `station_id`**, mọi bản ghi của cùng một trạm đo luôn được hash vào cùng 1 partition → **bảo toàn thứ tự thời gian theo từng trạm**, đồng thời chia tải đều trên 3 partitions để mở rộng lên tối đa 3 Consumer chạy song song trong cùng group ở pha BTL.

### Chặng 3 — Gom lô, Kiểm định dữ liệu & Cảnh báo ngưỡng (`consumer/app/main.py`, `validator.py`)

1. **Vòng lặp gom buffer (`main.py`)**:
   - Consumer tắt `enable.auto.commit=False`, thiết lập `auto.offset.reset="earliest"`, `session.timeout.ms=30000`.
   - `consumer.poll(timeout=0.1)` nhận từng bản tin đưa vào `buffer`.
   - Hàm `flush_buffer()` được kích hoạt theo **ngưỡng kép (dual-trigger)**: ngay khi **`len(buffer) >= batch_size`** (100 ở `batch`, 1 ở `single`) **HOẶC** thời gian chờ của phần tử đầu tiên trong buffer vượt **`batch_timeout_s`** (`500 ms` mặc định). Điều này đảm bảo ở tải thấp dữ liệu vẫn được đẩy ngay sau tối đa `0.5 giây`, không bị kẹt chờ đủ 100 dòng.
2. **Kiểm định & Lọc nhiễu (`_decode_message` + `validate_event`)**:
   Mỗi bản tin được phân loại vào `valid_records` hoặc `rejected_records` theo thứ tự kiểm tra chặt chẽ:
   - `EMPTY_MESSAGE_VALUE`: payload `None`.
   - `INVALID_JSON`: không giải mã được UTF-8 hoặc JSON lỗi cú pháp.
   - `INVALID_PAYLOAD_TYPE`: payload không phải JSON object (`dict`).
   - `MISSING_REQUIRED_FIELD:<tên_trường>`: thiếu hoặc rỗng bất kỳ trường nào trong 6 trường bắt buộc (`run_id`, `event_id`, `station_id`, `determinand`, `event_time`, `produced_at`).
   - `NON_MEASUREMENT_CODED`: `determinand == "NO FLOW/SAMP"` hoặc `unit.lower() == "coded"`.
   - `INVALID_RESULT_VALUE`: `result_raw` không phải số thực hữu hạn (`NaN`, `inf`, chuỗi rỗng hoặc chữ).
   - `NEGATIVE_VALUE`: giá trị `< 0` cho các chỉ tiêu không phải `Temp Water`.
   - `OUT_OF_PHYSICAL_RANGE`: `pH` ngoài `[0.0, 14.0]` hoặc `Temp Water` ngoài `[-5.0, 60.0]`.
   - `INVALID_EVENT_TIME` / `INVALID_PRODUCED_AT`: sai chuẩn ISO-8601.
3. **Phân loại cảnh báo (`classify_alert`)** cho 7 chỉ tiêu chuẩn:

| Chỉ tiêu (`determinand`) | Ngưỡng `NORMAL` | Ngưỡng `WARNING` | Ngưỡng `CRITICAL` |
| :--- | :--- | :--- | :--- |
| `BOD ATU` | `<= 25.0` | `> 25.0` đến `50.0` | `> 50.0` |
| `Ammonia(N)` | `<= 5.0` | `> 5.0` đến `15.0` | `> 15.0` |
| `Sld Sus@105C` | `<= 35.0` | `> 35.0` đến `75.0` | `> 75.0` |
| `COD as O2` | `<= 125.0` | `> 125.0` đến `250.0` | `> 250.0` |
| `Phosphorus-P` | `<= 2.0` | `> 2.0` đến `5.0` | `> 5.0` |
| `pH` | `6.0` đến `9.0` | `< 6.0` hoặc `> 9.0` (trong `[5.0, 10.0]`) | `< 5.0` hoặc `> 10.0` |
| `Temp Water` | `<= 25.0` | `> 25.0` đến `30.0` | `> 30.0` |

### Chặng 4 — Ghi Batch Idempotent bằng `UNNEST` (`consumer/app/db.py`)

1. **Chuyển vị theo cột**: `pack_valid_columns()` biến danh sách `N` đối tượng `ValidMeasurement` thành `16` mảng cột; `pack_rejected_columns()` biến các dòng lỗi thành `6` mảng cột (kèm `Jsonb(raw_payload)`).
2. **Ghi nguyên tử trong 1 transaction duy nhất (`write_batch`)**:
   - Thực thi `INSERT INTO water_measurements (...) SELECT * FROM unnest(...) ON CONFLICT (run_id, event_id) DO NOTHING RETURNING event_id` và tương tự cho `rejected_events`, rồi mới gọi `conn.commit()`.
   - Số bản ghi mới chèn được đếm trực tiếp từ `RETURNING event_id`; số bản ghi trùng bị bỏ qua tính bằng `deduped_valid = len(valid_records) - inserted_valid`.
   - Cột `ingested_at` không truyền từ Python mà do PostgreSQL gán bằng `clock_timestamp()` ngay lúc ghi từng dòng.
3. **Tương thích PgBouncer & Tự phục hồi kết nối**:
   - Kết nối luôn đặt `prepare_threshold=None` để không xung đột prepared statement khi qua PgBouncer pooled endpoint của Neon.
   - Nếu gặp `psycopg.OperationalError` (mất mạng thoáng qua hoặc Neon đang thức dậy từ trạng thái ngủ), `DatabaseWriter` đóng kết nối cũ và thử lại tối đa `4` lần theo exponential backoff (`1s`, `2s`, `4s`, `8s`).

### Chặng 5 — Manual Offset Commit & Cơ chế Crash Recovery (`exit code 77`)

Nguyên tắc sống còn để bảo đảm **không mất dữ liệu khi Consumer sập**:
> Thứ tự bắt buộc trong `flush_buffer()`: **Ghi & `conn.commit()` xuống PostgreSQL TRƯỚC → Commit Kafka offset SAU** (`consumer.commit(asynchronous=False)`).

```mermaid
sequenceDiagram
    autonumber
    participant K as Kafka Broker
    participant C1 as Consumer (bị tiêm lỗi crash)
    participant DB as PostgreSQL / Neon
    participant C2 as Consumer (khởi động lại)

    K->>C1: Giao batch (ví dụ sự kiện e1)
    C1->>DB: INSERT ... ON CONFLICT DO NOTHING + conn.commit()
    DB-->>C1: DB đã lưu bền vững e1
    Note over C1: CONSUMER_CRASH_AFTER_DB_COMMIT=true<br/>Log INJECTED_CRASH_AFTER_DB_COMMIT<br/>os._exit(77) NGAY TRƯỚC consumer.commit()!
    Note over K: Kafka KHÔNG nhận được offset commit cho e1

    C2->>K: Subscribe lại consumer group water-quality-consumer
    K->>C2: Phát lại từ offset chưa commit (gồm cả e1 + e2..e100)
    C2->>DB: Batch UNNEST [e1, e2..e100] ON CONFLICT (run_id, event_id) DO NOTHING
    DB-->>C2: Bỏ qua e1 (đã có), chèn mới e2..e100 -> RETURNING 99 dòng mới
    Note over C2: Log DEDUP_REPLAY (deduped_count > 0) nếu có bản ghi hợp lệ bị trùng
    C2->>K: consumer.commit(asynchronous=False)
    Note over K,DB: Kết quả cuối cùng: 0 dòng mất, 0 dòng trùng, kafka_lag = 0
```

Cơ chế này chứng minh hệ thống đạt **At-Least-Once Delivery** (từ Kafka) kết hợp **Idempotent Write** (tại PostgreSQL qua khóa chính `(run_id, event_id)` + `ON CONFLICT DO NOTHING`).
*(Lưu ý triển khai trong `consumer/app/main.py`: cảnh báo log `DEDUP_REPLAY` được in khi có bản ghi hợp lệ bị replay `deduped_valid > 0`; nếu bản ghi bị crash trước đó rơi vào bảng `rejected_events` như `NO FLOW/SAMP`, `ON CONFLICT DO NOTHING` vẫn chống trùng âm thầm và `BATCH_COMMITTED` ghi nhận `inserted_rejected` giảm đi 1.)*

### Chặng 6 — Quan sát Grafana 10 Panels & Đánh giá bằng `evaluate_run.py`

1. **Ba mốc thời gian tách biệt trên DB**:
   - `event_time`: thời điểm lấy mẫu năm 2025 từ file CSV.
   - `produced_at`: thời điểm UTC Producer đẩy lên Kafka.
   - `ingested_at`: thời điểm `clock_timestamp()` PostgreSQL ghi nhận an toàn.
   - **End-to-End Latency** = `EXTRACT(EPOCH FROM (ingested_at - produced_at)) * 1000.0` (ms).
2. **10 Panels trên Dashboard `Water Quality Cloud Monitoring (N12-L01)` (`uid: wq-cloud-n12-l01`, refresh `5s`)**, lọc động theo 2 biến `$run_id` và `$determinand`:
   - *Panel 1 (Stat)*: `Total Ingested Measurements`
   - *Panel 2 (Stat)*: `Rejected / Noise Events`
   - *Panel 3 (Stat)*: `Threshold Alerts (WARN / CRIT)`
   - *Panel 4 (Stat)*: `P95 End-to-End Latency`
   - *Panel 5 (Timeseries)*: `Ingestion Throughput (events/sec by ingested_at)`
   - *Panel 6 (Timeseries)*: `End-to-End Pipeline Latency (Producer -> Neon DB)`
   - *Panel 7 (Timeseries)*: `Live Water Quality Values (by Ingestion Time)`
   - *Panel 8 (Barchart)*: `2025 Historical Sampling Distribution (by Event Month)`
   - *Panel 9 (Table)*: `Alert Breakdown by Determinand & Level`
   - *Panel 10 (Table)*: `Latest Ingested Water-Quality Events`
   *(Các Panel 5, 6, 7 lọc theo khung thời gian `$__timeFilter(ingested_at)` của Grafana; các Panel 1–4, 8, 9, 10 tổng hợp toàn bộ dữ liệu theo `$run_id` và `$determinand` không phụ thuộc bộ chọn thời gian.)*
3. **Đo kiểm tự động (`benchmarks/evaluate_run.py`)**:
   - Chờ đủ số bản ghi kỳ vọng cập bến DB, truy vấn `COUNT(*)`, `COUNT(DISTINCT event_id)`, `drain_span_s`, `p50/p95/max` E2E latency và đếm mức cảnh báo.
   - Nếu truyền `--bootstrap`, gọi `AdminClient.list_consumer_group_offsets()` đối chiếu với `get_watermark_offsets()` trên từng partition để tính chính xác `kafka_lag`, rồi xuất báo cáo JSON vào `benchmarks/results/`.
