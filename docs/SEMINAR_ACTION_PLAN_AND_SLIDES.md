# Kế hoạch 4 ngày hoàn thành Seminar, Sửa 12 Slide & Kịch bản Demo (Nhóm 12 - L01)

Tài liệu này tổng hợp rà soát đối chiếu giữa:
- Đề cương: [cloud_seminar.docx](../../cloud_seminar.docx)
- Slide hiện tại: [Seminar-N12-L01.pdf](../../Seminar-N12-L01.pdf)
- Code & dữ liệu thực tế: [water-quality-cloud/](../) và [Đánh giá mô hình/](../../Đánh%20giá%20mô%20hình/)

---

## 1. Kế hoạch hành động 4 ngày & Chia việc 5 thành viên

| Thành viên | Vai trò chính (4 ngày) | Sản phẩm bàn giao |
| :--- | :--- | :--- |
| **Tào Nguyễn Tâm Phúc** | **Cloud Infra & Deploy Lead** | Tạo Neon DB + AWS EC2 `t3.medium`, thiết lập Security Group, chạy `compose.yaml`, giữ IP/SSH key & điều phối demo live |
| **Phan Gia Phúc** | **Pipeline & Benchmark Lead** | Chạy `replay_producer.py`, `run_seminar_benchmarks.ps1` (4 phép đo thật), thu log `exit code 77` & `DEDUP_REPLAY`, xuất 2 ảnh PNG mới |
| **Trịnh Bá Học** | **Grafana & Security/Cost Lead** | Kiểm tra dashboard Grafana live trên EC2, chụp screenshot demo & `Test-NetConnection`, lập bảng chi phí thực tế tại `calculator.aws` + `neon.com/pricing` |
| **Trần Thiện Huy** | **Slide Architect (Slides 1–8)** | Sửa toàn bộ nội dung, typo, biểu đồ kiến trúc Slides 1–8 cho khớp tuyệt đối với pipeline đã chạy thật |
| **Trần Trường Sinh** | **Slide Architect (Slides 9–12) & Demo Backup** | Thay nền template Slides 9–11, chèn ảnh/số liệu Cloud mới, hoàn thiện Slide 12 (hạn chế + trích dẫn), quay video backup 2 phút |

### Lịch trình 4 ngày (khuyến nghị)

- **Ngày 1 (Hạ tầng & Chạy được luồng đầu tiên)**:
  - Tâm Phúc tạo Neon (`ap-southeast-1`) + chạy `db/init_db.py`; tạo EC2 `t3.medium` + `docker compose up -d --build`.
  - Gia Phúc test SSH tunnel + chạy `replay_producer.py --rate 20 --max-events 200`, Bá Học xác nhận dữ liệu lên Grafana.
- **Ngày 2 (Đo 4 minh chứng Cloud & Thu log/chi phí)**:
  - Gia Phúc + Tâm Phúc chạy trọn `run_seminar_benchmarks.ps1 -Phase all`, lưu JSON kết quả thật và sinh `slide9_cloud_throughput.png`, `slide10_cloud_recovery.png`.
  - Bá Học chạy kiểm tra cổng bảo mật (`Test-NetConnection`), chụp log Neon TLS & tính chi phí tháng trên AWS Pricing Calculator + Neon Pricing (ghi rõ ngày tra giá).
- **Ngày 3 (Sửa dứt điểm 12 Slide & Quay video dự phòng)**:
  - Thiện Huy sửa Slides 1–8; Trường Sinh sửa Slides 9–12, xóa sạch text template nền cũ.
  - Quay sẵn 1 video màn hình full HD (2 phút) cảnh chạy thật Producer → EC2 logs → Neon → Grafana (phòng mạng phòng học chập chờn).
- **Ngày 4 (Tổng duyệt 2 lượt & Q&A)**:
  - Chạy thử toàn bộ bài thuyết trình bấm giờ (< 15 phút gồm cả demo 4–5 phút) + tập trả lời 10 câu hỏi GVHD ở Mục 5.

---

## 2. Chi tiết rà soát & Sửa từng Slide (Slides 1–12)

### Slide 1 — Giới thiệu đề tài
- **Lỗi phát hiện ở PDF hiện tại**: Tên thành viên thứ 5 đang ghi `Trân Trường Sinh` (thiếu dấu huyền chữ `Trần`!).
- **Cần sửa**: Sửa lại `Trần Trường Sinh`. Kiểm tra tên môn học, mã nhóm `N12-L01`, GVHD `Nguyễn Quang Hùng`.

### Slide 2 — Mục tiêu
- **Lỗi phát hiện**: Ô thứ 2 ghi *"Ứng dụng mô hình ML/AI để điền khuyết và dự đoán"* ngang hàng với pipeline, nhưng phần sau chưa trình bày ML khiến GVHD dễ hỏi *"ML đâu?"*.
- **Cần sửa**: Phân rõ **2 pha trong cùng một kiến trúc mở rộng**:
  - **Pha 1 (Seminar — Hoàn thành & Kiểm chứng Cloud)**: Thiết kế, triển khai và đo kiểm pipeline streaming thời gian thực trên AWS EC2 + Neon PostgreSQL + Grafana.
  - **Pha 2 (Bài tập lớn — Mở rộng)**: Tích hợp Cloud-native Elasticity (ASG/ECS, Terraform, Secrets Manager) và ML Worker dự đoán/điền khuyết `BOD ATU`, `Ammonia(N)` (mở rộng từ bộ script baseline trong `Đánh giá mô hình/`).

### Slide 3 — Bài toán và dữ liệu
- **Cần bổ sung từ kết quả profile dữ liệu thật**:
  - Bộ dữ liệu `2025-C` có **177.319 bản ghi**, **7.371 trạm**, **483 chỉ tiêu** (từ 01/2025 – 09/2025).
  - Tập trung 7 chỉ tiêu cốt lõi + lọc dòng trạng thái coded (`NO FLOW/SAMP`: 2.993 dòng; `BOD ATU`: 44.155; `Sld Sus@105C`: 28.582; `Ammonia(N)`: 21.533; `COD as O2`: 17.684; `Phosphorus-P`: 17.196; `Temp Water`: 7.439; `pH`: 4.231).
  - Ghi rõ: mỗi dòng CSV là **1 chỉ tiêu đơn lẻ tại 1 trạm/thời điểm**, phát lại tuần tự (replay) với `station_id` làm Kafka partition key.

### Slide 4 — Vai trò của điện toán đám mây
- **Cần sửa**: Chuyển phụ đề *"Các lợi ích kỳ vọng — cần được kiểm chứng bằng triển khai"* thành *"Các lợi ích kiến trúc — đã triển khai và kiểm chứng trên AWS EC2 & Neon"*. Liên hệ trực tiếp 3 điểm (01: Triển khai từ xa Grafana/Consumer qua Internet; 02: Tách trách nhiệm IaaS vs Managed DB; 03: Dễ quan sát thời gian thực).

### Slide 5 — Mô hình dịch vụ và trách nhiệm quản lý
- **Cần giữ & làm rõ thêm**: Bảng EC2 (IaaS) vs Neon (Managed DB) rất tốt. Bổ sung 1 dòng giải thích thêm khái niệm **Scalability vs Elasticity** (như `cloud_seminar.docx` yêu cầu):
  - *Scalability* (đã thiết kế sẵn trong Seminar): topic chia **3 partitions** theo `station_id`, Consumer stateless qua biến môi trường để sẵn sàng scale ngang nhiều Consumer cùng group.
  - *Elasticity* (sẽ tự động hóa ở BTL): tự co giãn số Consumer theo Kafka Lag/CPU trên AWS ASG/ECS; Neon Serverless DB có khả năng scale compute/suspend tự động ở tầng Managed DB.

### Slide 6 — Kiến trúc xử lý đề xuất (Data Pipeline)
- **Lỗi nghiêm trọng cần sửa ngay**:
  1. Ô số 5 trên sơ đồ đang ghi nhầm **`PostgreSQL / Neon Serverless DB`** lặp lại 2 lần thay vì **`Grafana / Real-time Dashboard`**. Sửa thành: `CSV Replay Producer` → `Kafka (EC2)` → `Python Consumer` → `PostgreSQL (Neon)` → `Grafana Dashboard`.
  2. Mục *"Consumer: Bắt sự kiện, lọc dữ liệu rác, tính toán WQI"* chưa khớp với dữ liệu đơn chỉ tiêu mỗi dòng. Sửa thành: **Validate schema, lọc nhiễu (`NO FLOW/SAMP`, giá trị phi vật lý), phân loại cảnh báo ngưỡng (`NORMAL` / `WARNING` / `CRITICAL`), và ghi batch idempotent (`UNNEST + ON CONFLICT DO NOTHING`)**.

### Slide 7 — Kiến trúc triển khai đề xuất (3 Phân vùng hạ tầng)
- **Cần sửa cho khớp triển khai thật**:
  - **Vùng 1 (Local Laptop)**: Producer gửi JSON qua **SSH Tunnel (`127.0.0.1:19092 → EC2:19092`)**, không mở cổng 9092 ra Internet.
  - **Vùng 2 (AWS EC2 `t3.medium`, `ap-southeast-1`, Ubuntu 24.04)**: Security Group chỉ mở **22 (My IP)** và **3000 (Grafana)**. Kafka KRaft (không Zookeeper, `3 partitions`, bind `127.0.0.1:19092`).
  - **Vùng 3 (Neon Cloud, `ap-southeast-1`)**: Serverless PostgreSQL 16 Pooled Connection, bắt buộc `sslmode=require`, `prepare_threshold=None`.

### Slide 8 — Luồng xử lý một bản ghi (End-to-End Sequence)
- **Lỗi typo nghiêm trọng ở bước 5**: Đang bị dính chữ **`trên đồ thị.mer Validate: Kiểm tra tính hợp lệ`**. Xóa phần text lỗi này!
- **Cần nâng cấp**: Phân biệt rõ **3 mốc thời gian** trong bảng `water_measurements` (rất ăn điểm khi demo Grafana):
  1. `event_time`: Thời điểm lấy mẫu thực địa năm 2025 (từ CSV).
  2. `produced_at`: Thời điểm Laptop Producer đẩy vào Kafka.
  3. `ingested_at`: Thời điểm Neon DB commit thành công (`clock_timestamp()`), dùng để đo **End-to-End Latency (`ingested_at - produced_at`)** trên Cloud.

### Slide 9 — Đánh giá hiệu năng & Nút thắt WAN trên Cloud
- **Lỗi hiện tại**: Ảnh cũ là số liệu local + còn lộ dòng text mẫu mờ phía dưới (*"GỢI Ý NỘI DUNG..."*).
- **Cần sửa**: Tạo lại slide sạch (không giữ khung template nháp), đặt ảnh **`benchmarks/charts/slide9_cloud_throughput.png`** đo thật trên AWS EC2 → Neon (`ap-southeast-1`), nhấn mạnh bài học Cloud thực tế:
  - Burst 300 sự kiện @ 10 / 50 / 100 evt/s vào Kafka trên EC2.
  - Tải duy trì 1.000 sự kiện @ 100 evt/s: **Single-commit qua WAN tới Neon bị nghẽn do RTT mạng**, còn **Batch-commit (`batch=100`, `UNNEST`)** giảm số round-trip đi 100 lần, xóa backlog và giảm p95 latency rõ rệt.

### Slide 10 — Phục hồi lỗi & Chống ghi trùng trên Cloud
- **Lỗi hiện tại**: Còn lộ chữ template nền cũ phía dưới.
- **Cần sửa**: Tạo slide sạch, đặt ảnh **`benchmarks/charts/slide10_cloud_recovery.png`** từ bài test crash thật trên EC2 (`CONSUMER_CRASH_AFTER_DB_COMMIT=true` → `exit code 77` trước commit offset → restart → log `DEDUP_REPLAY` → `0 trùng, lag = 0`). Giải thích rõ cơ chế đạt **At-Least-Once + Idempotent Write** qua `PRIMARY KEY (run_id, event_id) + ON CONFLICT DO NOTHING`.

### Slide 11 — Bảo mật & Chi phí triển khai thực tế trên Cloud
- **Lỗi hiện tại**: Đang ghi kết quả local Docker trên MSI (*"Chưa phát sinh hóa đơn cloud"*).
- **Cần sửa**: Chia 2 cột gọn gàng:
  - **Cột trái — Bảo mật đã kiểm chứng trên Cloud**:
    1. Security Group EC2 chỉ mở cổng `22` (My IP) và `3000` (Grafana); kiểm chứng bằng `Test-NetConnection` cổng `9092/19092` bị chặn từ Internet (`TcpTestSucceeded: False`).
    2. Kafka chỉ bind `127.0.0.1:19092`; Laptop kết nối qua **SSH Tunnel** mã hóa.
    3. Kết nối EC2/Grafana → Neon bắt buộc `sslmode=require` (TLS 5432); toàn bộ mật khẩu nằm trong `.env` (`.gitignore` loại khỏi Git).
    4. Container Consumer chạy user non-root (`appuser`).
  - **Cột phải — Bảng ước tính chi phí thực tế (`ap-southeast-1`, kiểm chứng ngày `<NGÀY_TRA_GIÁ>`)**:
    *(Nhóm tự tra trực tiếp trên `https://calculator.aws` và `https://neon.com/pricing` để điền con số chính xác ngày nộp, không bịa số!)*

    | Thành phần | Cấu hình thực tế | Nguồn tra giá | Chi phí ước tính (tháng / demo) |
    | :--- | :--- | :--- | :--- |
    | AWS EC2 Compute | `t3.medium` (2 vCPU, 4GB RAM), Ubuntu 24.04, `ap-southeast-1` | `calculator.aws` (On-Demand) | `<điền $/giờ × số giờ demo hoặc 730h/tháng>` |
    | AWS EBS Storage | `20 GiB gp3` | `calculator.aws` | `<điền $/tháng>` |
    | Network Egress | Outbound dữ liệu nhỏ (Grafana + DB writes) | `calculator.aws` | `<điền $/tháng>` |
    | Neon Managed DB | Free Tier (`0.5 GB storage`, auto-suspend) | `neon.com/pricing` | `$0 / tháng` (nếu vượt: Launch plan) |

### Slide 12 — Kết luận, Hạn chế & Tài liệu tham khảo
- **Cần sửa**:
  - Chốt lại những gì **đã làm và đo kiểm thật trên Cloud** (EC2 + Neon + Grafana live, Batch vs Single over WAN, Crash-recovery 0 duplicates, SSH tunnel security).
  - Nêu thẳng thắn **hạn chế hiện tại & hướng BTL**:
    - Hiện mới chạy 1 Kafka broker (`RF=1`), 1 EC2 chưa có Auto Scaling thật, Grafana chưa bọc HTTPS reverse proxy, và ML/AI mới ở mức thử nghiệm offline (`Đánh giá mô hình/`).
    - Sang **BTL**: Terraform IaC + Multi-consumer Elasticity + Secrets Manager/HTTPS + tích hợp ML Worker (`BOD ATU`, `Ammonia(N)`).
  - Bổ sung **Tài liệu tham khảo** đầy đủ:
    1. UK Environment Agency Water Quality Archive (`environment.data.gov.uk/water-quality`).
    2. Apache Kafka Documentation — Delivery Semantics & Consumer Groups (`kafka.apache.org/documentation`).
    3. PostgreSQL 16 Documentation — `INSERT ... ON CONFLICT` & `UNNEST`.
    4. AWS EC2 Security Groups & Pricing Calculator (`calculator.aws`); Neon Serverless Docs (`neon.tech/docs`).
    5. Pazo et al. (2024), *Enhancing water quality prediction for fluctuating missing data scenarios*, Science of the Total Environment 927 (`1-s2.0-S0048969724024835-main.pdf`).
    6. Kimura et al. (2022), *Fast Mining and Forecasting of Co-evolving Epidemiological Data Streams*, KDD (`3534678.3539078.pdf`).

---

## 3. Kịch bản Demo Live 5 phút trước GVHD

1. **Chuẩn bị trước giờ G (15 phút trước khi lên bảng)**:
   - Start EC2 instance, SSH vào kiểm tra `docker compose ps` (4 service sạch sẽ), xóa/kiểm tra dashboard Grafana (`http://<EC2_IP>:3000`) mở sẵn trên 1 tab trình duyệt, chọn filter `$run_id` là `All`.
   - Mở sẵn 2 cửa sổ terminal cạnh nhau: Terminal 1 giữ SSH Tunnel (`ssh -i ... -N -L 19092:127.0.0.1:19092 ubuntu@<EC2_IP>`), Terminal 2 đặt sẵn lệnh chạy Producer + 1 tab SSH EC2 chạy `docker logs -f wq-consumer`.
2. **Thao tác demo (4–5 phút)**:
   - **Phút 1**: Giải thích nhanh Security Group (22 + 3000) & SSH Tunnel đang bọc lưu lượng từ Laptop lên EC2.
   - **Phút 2**: Chạy `python producer\replay_producer.py --rate 25 --max-events 300 --run-id demo-live`. Chỉ cho thầy thấy log `BATCH_COMMITTED` trên EC2 container đang đẩy thẳng lên Neon Cloud.
   - **Phút 3**: Chuyển sang Grafana bấm Refresh (hoặc chờ auto-refresh 5s): chỉ **Total Ingested**, **Rejected Noise (`NO FLOW/SAMP`)**, **Threshold Alerts (`WARNING`/`CRITICAL`)**, **P95 End-to-End Latency**, và **2 biểu đồ thời gian** (`ingested_at` chạy live vs `event_time` 2025).
   - **Phút 4–5**: Mở sẵn terminal/ảnh chứng minh kết quả Single vs Batch và Crash-recovery (`exit code 77` + `DEDUP_REPLAY`, `0 trùng`).
3. **Phương án dự phòng (nếu Wi-Fi phòng học chặn SSH port 22 hoặc mất mạng)**:
   - Mở ngay **video quay màn hình Full HD** đã thực hiện ở Ngày 3 (chỉ rõ IP EC2 thật và Neon Console) để trình chiếu mượt không bị gián đoạn giờ báo cáo; hoặc bật `compose.local.yaml` nếu cần chạy pipeline ngay trên máy.

---

## 4. Top 10 câu hỏi GVHD hay vặn nhất & Gợi ý trả lời

1. **Tại sao để Producer trên Laptop chứ không đưa luôn lên EC2?**
   - *Trả lời*: Laptop đóng vai trò mô phỏng Edge/IoT Gateway ở ngoài thực địa gửi sự kiện qua mạng công cộng vào Cloud, giúp kiểm chứng đúng ranh giới mạng, giao thức kết nối bảo mật (SSH tunnel), và phân tách vùng sản sinh dữ liệu với vùng xử lý trên Cloud.
2. **Nếu Laptop gửi qua mạng công cộng mà Kafka chỉ dùng `PLAINTEXT` trên `127.0.0.1:19092` thì có an toàn không?**
   - *Trả lời*: Rất an toàn ở pha Seminar vì cổng `19092` chỉ bind vào loopback `127.0.0.1` bên trong EC2 và Security Group AWS khóa hoàn toàn cổng 9092/19092. Toàn bộ dữ liệu đi qua Internet được mã hóa bên trong **SSH Tunnel (port 22)**. Sang BTL sẽ nâng cấp lên `SASL_SSL` hoặc FastAPI HTTPS Ingestion Gateway để không phụ thuộc khóa SSH.
3. **Tại sao dùng Kafka KRaft thay vì bản Kafka truyền thống có Zookeeper?**
   - *Trả lời*: KRaft bỏ hoàn toàn cụm Zookeeper riêng, giảm tiêu thụ RAM và số container trên EC2 `t3.medium` (4GB RAM), đơn giản hóa vận hành và khớp chuẩn kiến trúc Kafka 3.x/4.x hiện đại.
4. **Tại sao chọn EC2 tự quản lý Kafka/Grafana kết hợp Neon Managed DB thay vì dùng toàn bộ AWS RDS hay tự cài Postgres trong Docker?**
   - *Trả lời*: Nếu cài Postgres chung Docker trên EC2 thì mất tính tách rời lưu trữ bền vững (stateless compute vs stateful storage) và khó chứng minh mô hình lai IaaS + Managed DB. Dùng Neon Serverless giúp tách trách nhiệm vận hành DB (TLS, backup, pooled connections, auto-suspend tiết kiệm phí) mà nhóm vẫn toàn quyền kiểm soát schema/SQL, đồng thời tạo ra phép đo độ trễ WAN thật giữa EC2 và DB ngoài VM.
5. **Tại sao single-commit trên Cloud lại tụt thông lượng mạnh so với batch-commit?**
   - *Trả lời*: Mỗi `INSERT` + `COMMIT` đơn lẻ tốn ít nhất 1 vòng RTT mạng WAN từ EC2 tới Neon (hàng chục ms). Ở tốc độ 100 evt/s, một consumer single-commit không thể hoàn tất 100 vòng RTT/giây nên tạo backlog. Khi gộp `batch=100` bằng một câu lệnh `UNNEST` duy nhất, số round-trip WAN giảm 100 lần nên thông lượng bắt kịp đầu vào.
6. **Hệ thống có đạt "exactly-once" thật sự không?**
   - *Trả lời*: Không tuyên bố "exactly-once tuyệt đối xuyên Kafka và PostgreSQL" (vì không dùng distributed 2PC giữa Kafka và Postgres). Thay vào đó, nhóm kết hợp **At-Least-Once Delivery** (tắt `auto.commit`, chỉ commit Kafka offset *sau* khi Neon DB commit thành công) + **Idempotent Write** (`PRIMARY KEY (run_id, event_id)` với `ON CONFLICT DO NOTHING`). Kết quả hiệu dụng lên dữ liệu đích là không mất và không trùng bản ghi.
7. **Khóa `event_id` lấy từ đâu, nếu dữ liệu thật có sự kiện trùng từ sensor thì sao?**
   - *Trả lời*: Lấy định danh duy nhất `@id` từ bộ dữ liệu Environment Agency (ví dụ `AN-2089657-0085`) kết hợp `run_id` phiên chạy. Chúng tôi đã kiểm tra trên toàn bộ 177.319 dòng CSV: không có `@id` nào trùng lặp trong dữ liệu gốc, nên nếu có duplicate xuất hiện trong cùng `run_id` thì chắc chắn là do replay khi lỗi/restart.
8. **Slide 2 nói có ML/AI và Slide 6 từng nhắc WQI, sao Demo Seminar lại chưa thấy?**
   - *Trả lời*: Sau khi khảo sát dữ liệu thật (`2025-C`), mỗi dòng CSV là một chỉ tiêu đơn lẻ tại thời điểm rời rạc chứ không đồng bộ đủ bộ chỉ tiêu để tính ngay WQI chuẩn, nên Consumer Seminar tập trung vào ETL chuẩn hóa, lọc dòng `NO FLOW/SAMP` và cảnh báo ngưỡng từng chỉ tiêu. Nhóm đã viết sẵn bảng `ml_predictions` trong Neon và đặt mảng ML/AI + Elasticity làm trọng tâm cho giai đoạn BTL tiếp theo.
9. **Hiện tại 1 EC2 và 1 Kafka broker (`RF=1`) chết thì sao? Có phải SPOF không?**
   - *Trả lời*: Đúng, đây là điểm đánh đổi có chủ đích ở quy mô Seminar để kiểm soát chi phí. Dữ liệu đã lưu trong Neon DB (Managed DB ngoài EC2) sẽ không mất khi EC2 sập, nhưng pipeline thu thập sẽ gián đoạn cho tới khi EC2/Kafka phục hồi. Đây chính là lý do BTL sẽ nâng cấp sang kiến trúc có Auto Scaling / ECS và tách trạng thái.
10. **Làm sao chứng minh 3 partitions của Kafka thực sự sẵn sàng cho việc mở rộng?**
    - *Trả lời*: Kafka topic `water-quality-events` được `kafka-init` khởi tạo cố định với `--partitions 3`, và Producer băm partition key theo `station_id`. Điều này bảo đảm thứ tự thời gian của từng trạm quan trắc luôn được giữ đúng trong 1 partition, đồng thời cho phép nâng ngay số lượng container Consumer lên tối đa 3 instance trong cùng consumer group ở BTL mà không cần sửa code Producer hay Consumer.
