# CIC-IDS-BigData

Big Data pipeline lưu trữ và xử lý dữ liệu network traffic quy mô lớn (bộ **CIC-IDS2017**, ~2.8 triệu flow) bằng **Hadoop HDFS, Hive và Apache Spark**, chạy hoàn toàn trên Docker.

```
CSV gốc ──► HDFS (/raw, /cleaned) ──┬──► Hive  (bảng raw + partition theo attack_type)
                                    │
                                    └──► Spark (cleaning ─► transform ─► analytics)
                                              │
                                              └──► HDFS /processed (Parquet + CSV) ──► Hive tables / CSV kết quả
```

## Mục lục
- [Cấu trúc thư mục](#cấu-trúc-thư-mục)
- [Yêu cầu & chuẩn bị dữ liệu](#yêu-cầu--chuẩn-bị-dữ-liệu)
- [Chạy nhanh bằng `run_pipeline.sh`](#chạy-nhanh-bằng-run_pipelinesh)
- [Web UI](#web-ui)
- [Dữ liệu trên HDFS & Hive tables](#dữ-liệu-trên-hdfs--hive-tables)
- [Spark jobs](#spark-jobs)
- [Chạy thủ công từng bước](#chạy-thủ-công-từng-bước)
- [Lưu ý về dữ liệu](#lưu-ý-về-dữ-liệu)
- [Xử lý lỗi thường gặp](#xử-lý-lỗi-thường-gặp)
- [Tắt / dọn dẹp](#tắt--dọn-dẹp)

---

## Cấu trúc thư mục

```
CIC-IDS-BigData/
├── run_pipeline.sh            # Dựng lại toàn bộ pipeline bằng 1 lệnh
├── data/
│   ├── data_cleaning.py       # Làm sạch cơ bản bằng pandas -> data/cleaned_data/
│   ├── raw_data/              # (tự đặt) 8 file CSV gốc - không commit
│   └── cleaned_data/          # (sinh ra) CSV đã làm sạch - không commit
├── hdfs/
│   ├── docker-compose.yml     # Hadoop + Hive + Spark + Jupyter
│   ├── hadoop.env.example     # Cấu hình Hadoop/Hive (copy thành hadoop.env)
│   └── hdfs_setup.sh          # Tạo cấu trúc thư mục trên HDFS
├── hive/
│   ├── create_tables.hql      # Bảng raw (external) + bảng partition theo attack_type
│   └── queries.hql            # Các truy vấn phân tích HiveQL
└── spark/
    ├── Dockerfile             # Image chung cho spark-master / worker / jupyter
    ├── conf/spark-defaults.conf
    ├── config.py              # SparkSession + hằng số đường dẫn HDFS
    ├── schema.py              # Schema 79 cột + map label -> attack_category
    ├── dimensions.py          # Bảng dimension (port -> service, attack category)
    ├── ingest_raw.py          # Upload CSV gốc lên HDFS /raw/<day>/
    ├── cleaning.py            # Cleaning bằng PySpark
    ├── transform.py           # Feature engineering, ghi Parquet/CSV, đăng ký Hive tables
    ├── analytics.py           # 15 truy vấn Spark SQL
    ├── smoke_test.py          # Kiểm tra Spark đọc được HDFS + Hive
    └── reports/               # (sinh ra) report JSON + kết quả analytics dạng CSV
```

## Yêu cầu & chuẩn bị dữ liệu

| Thành phần | Ghi chú |
|---|---|
| Docker Desktop + Docker Compose v2 | Cấp cho Docker **≥ 8 GB RAM** (cluster có 11 container) |
| Python 3.x + `pandas`, `numpy` | Cho bước làm sạch bằng pandas: `pip install pandas numpy` |
| Git Bash (Windows) hoặc bash (Linux/macOS) | Để chạy `run_pipeline.sh` |

**Dữ liệu:** tải bộ **MachineLearningCSV** của CIC-IDS2017 (8 file, ~850 MB) từ trang của [Canadian Institute for Cybersecurity](https://www.unb.ca/cic/datasets/ids-2017.html) và giải nén vào `data/raw_data/`:

```
data/raw_data/
├── Monday-WorkingHours.pcap_ISCX.csv
├── Tuesday-WorkingHours.pcap_ISCX.csv
├── Wednesday-workingHours.pcap_ISCX.csv
├── Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv
├── Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv
├── Friday-WorkingHours-Morning.pcap_ISCX.csv
├── Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv
└── Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv
```

> Tên file phải bắt đầu bằng thứ trong tuần (Monday...Friday) vì `ingest_raw.py` dùng nó để xếp vào `/raw/<day>/`.

---

## Chạy nhanh bằng `run_pipeline.sh`

Từ thư mục gốc repo (Git Bash trên Windows):

```bash
./run_pipeline.sh                    # chạy toàn bộ (~10-12 phút)
./run_pipeline.sh --from spark       # chạy từ một bước trở đi
./run_pipeline.sh --only analytics   # chỉ chạy một bước
```

| Bước | Việc làm |
|---|---|
| `cluster` | Tạo `hdfs/hadoop.env` nếu chưa có, build image Spark, `docker compose up -d`, chờ HDFS / DataNode / Hive metastore / 2 Spark worker sẵn sàng |
| `pandas` | Chạy `data/data_cleaning.py` (bỏ qua nếu `data/cleaned_data/` đã có file) |
| `hdfs` | Chạy `hdfs_setup.sh`, upload `cleaned_data/*.csv` lên `/cic-ids2017/cleaned/` |
| `hive` | Chạy `hive/create_tables.hql` (log: `spark/reports/hive_create_tables.log`) |
| `spark` | `ingest_raw.py` → `cleaning.py` → `transform.py` |
| `analytics` | `analytics.py` (15 truy vấn Spark SQL) |

Mọi bước đều **idempotent** – chạy lại không làm trùng dữ liệu. Bước nào lỗi script sẽ dừng và báo đỏ.

## Web UI

| Service | URL |
|---|---|
| HDFS NameNode | http://localhost:9870 |
| Spark Master (xem worker, application) | http://localhost:8080 |
| Spark Worker 1 / 2 | http://localhost:8081 · http://localhost:8082 |
| Spark job đang chạy | http://localhost:4040 |
| JupyterLab (không cần token) | http://localhost:8888 |
| HiveServer2 (JDBC) | `jdbc:hive2://localhost:10000` |

Trong notebook Jupyter:

```python
from config import get_spark
spark = get_spark()
spark.table("cic_ids2017.flows").groupBy("attack_category").count().show()
```

---

## Dữ liệu trên HDFS & Hive tables

```
/cic-ids2017/
├── raw/<monday..friday>/*.csv            # CSV gốc (ingest_raw.py)
├── cleaned/*.csv                         # CSV làm sạch bằng pandas (Hive: network_traffic_raw)
└── processed/
    ├── cleaned_spark/day=<day>/          # Output cleaning.py (Parquet)
    ├── flows/attack_category=<cat>/      # Output transform.py (Parquet)
    ├── flows_csv/attack_category=<cat>/  # Cùng dữ liệu dạng CSV (benchmark CSV vs Parquet)
    ├── dim_port_service/
    ├── dim_attack_category/
    └── analytics/<query>/                # Kết quả analytics.py
```

Database `cic_ids2017`:

| Table | Tạo bởi | Mô tả |
|---|---|---|
| `network_traffic_raw` | `create_tables.hql` | External table trên `/cleaned`, 79 cột |
| `network_traffic_partitioned` | `create_tables.hql` | Parquet, 24 cột, partition theo `attack_type` (nhãn gốc) |
| `flows` | `transform.py` | Parquet, dữ liệu đã xử lý + feature, partition theo `attack_category` |
| `flows_csv` | `transform.py` | Giống `flows` nhưng lưu dạng CSV |
| `dim_port_service` | `transform.py` | `destination_port` → `service`, `service_group` |
| `dim_attack_category` | `transform.py` | Mô tả + mức độ nghiêm trọng của nhóm tấn công |

Các bảng đọc được từ **cả Hive lẫn Spark SQL** (dùng chung Hive metastore).

---

## Spark jobs

| Script | Input | Output | Thời gian* |
|---|---|---|---|
| `ingest_raw.py` | `data/raw_data/*.csv` (local) | `/raw/<day>/` | ~30 s |
| `cleaning.py` | `/raw/` | `/processed/cleaned_spark/`, `reports/cleaning_report.json` | ~4-5 phút |
| `transform.py` | `/processed/cleaned_spark/` | `/processed/flows*`, `dim_*`, Hive tables, `reports/transform_report.json` | ~3-4 phút |
| `analytics.py` | Hive `flows`, `dim_*` | `/processed/analytics/`, `reports/analytics/*.csv`, `reports/analytics_timings.json` | ~2 phút |

\* Trên 2 worker × 2 core × 3 GB.

**Cleaning** (`cleaning.py`): kiểm tra header từng file với schema tường minh → gắn `source_file`, `day`, `session`, `seq_index` (vị trí dòng trong file gốc) → chuẩn hóa label (sửa ký tự lỗi encoding) và map sang 9 `attack_category` → Infinity/NaN thành null rồi loại → loại giá trị âm vô lý → loại duplicate trên toàn dataset.

| Bước | Số dòng |
|---|---|
| Raw | 2,830,743 |
| Sau loại null/inf | 2,827,876 |
| Sau loại giá trị âm | 2,827,761 |
| Sau loại duplicate | **2,520,691** |

**Transform** (`transform.py`): loại 8 cột hằng số → feature thời gian giả lập (`time_bucket`, `pseudo_hour`, `pseudo_time`) → feature mạng (`port_range`, `service` qua JOIN dimension) → feature lưu lượng (`bytes_per_packet`, `fwd_bwd_packet_ratio`, `fwd_bytes_share`, `has_response`...) → ghi Parquet (352 MB) và CSV (1.2 GB) → đăng ký Hive tables.

**Analytics** (`analytics.py`) – 15 truy vấn, chạy riêng được: `spark-submit analytics.py <tên query> ...`

| Nhóm | Query |
|---|---|
| Phân bố | `dist_label`, `dist_category`, `dist_day` |
| Thời gian | `temporal_bucket_overview`, `temporal_bucket_category`, `temporal_label_timeline`, `temporal_hourly` |
| Mạng | `network_top_ports`, `network_ports_per_attack`, `network_port_range`, `network_port_diversity`, `network_service_group` |
| So sánh | `compare_label_profile`, `compare_vs_benign`, `compare_tcp_flags` |

---

## Chạy thủ công từng bước

Dùng khi muốn hiểu / debug từng bước. Các lệnh chạy trong **PowerShell** tại thư mục `hdfs/`.

**1. Làm sạch bằng pandas**
```powershell
python ..\data\data_cleaning.py
```

**2. Khởi động cluster** (lần đầu cần `build`)
```powershell
Copy-Item hadoop.env.example hadoop.env   # nếu chưa có
docker compose build
docker compose up -d
docker ps                                 # phải thấy 11 container
```

**3. Upload lên HDFS**
```powershell
docker exec namenode rm -rf /cleaned_data            # tránh lồng thư mục khi copy lần 2
docker cp ..\data\cleaned_data namenode:/cleaned_data
docker cp hdfs_setup.sh namenode:/hdfs_setup.sh
docker exec namenode bash -c "sed -i 's/\r$//' /hdfs_setup.sh && bash /hdfs_setup.sh"
docker exec namenode bash -c "hdfs dfs -put -f /cleaned_data/*.csv /cic-ids2017/cleaned/"
```

**4. Hive**
```powershell
docker cp ..\hive\create_tables.hql hive-server:/create_tables.hql
docker cp ..\hive\queries.hql hive-server:/queries.hql
docker exec -it hive-server hive -f /create_tables.hql
docker exec -it hive-server hive -f /queries.hql
```

**5. Spark**
```powershell
docker exec jupyter spark-submit /opt/project/spark/ingest_raw.py
docker exec jupyter spark-submit /opt/project/spark/cleaning.py
docker exec jupyter spark-submit /opt/project/spark/transform.py
docker exec jupyter spark-submit /opt/project/spark/analytics.py
```

> Repo được mount vào `/opt/project` trong các container Spark, nên sửa code trên máy là chạy được ngay, không cần `docker cp`.

---

## Lưu ý về dữ liệu

- **MachineLearningCSV không có `Source IP`, `Destination IP`, `Protocol`, `Timestamp`.** Network analysis dựa trên `destination_port` (+ service suy ra từ port).
- **Thời gian là giả lập.** `seq_index` là vị trí dòng trong file gốc; `pseudo_time` trải đều `seq_index` lên khung giờ của session. Thứ tự các đợt tấn công khớp với lịch CIC công bố, nhưng **giờ tuyệt đối không chính xác** (các đợt flood sinh nhiều dòng trong thời gian ngắn) → nên phân tích theo thứ tự / bucket tương đối.
- **Pandas vs Spark cleaning cho số dòng khác nhau** (2,572,640 vs 2,520,691, chênh lệch hoàn toàn ở BENIGN): pandas loại duplicate trong từng file, Spark loại trên toàn dataset.
- Partition trên HDFS dùng dạng `attack_category=dos/` (chuẩn Hive/Spark), không phải `dos/`.

## Xử lý lỗi thường gặp

| Lỗi | Nguyên nhân / cách sửa |
|---|---|
| `compose file ... hadoop.env: no such file` | Chưa có `hdfs/hadoop.env` → copy từ `hadoop.env.example` |
| `Not a file: hdfs://.../cleaned/cleaned_data` | `docker cp` lần 2 tạo thư mục lồng → xóa `/cic-ids2017/cleaned/*` và upload lại chỉ `*.csv` (bước 3) |
| `bash: $'\r': command not found` | File `.sh` bị CRLF → `sed -i 's/\r$//' <file>` (đã có `.gitattributes` để giữ LF) |
| `Path does not exist: hdfs://...` sau khi khởi động lại | Volume Docker đã bị xóa (`down -v`) → chạy lại `./run_pipeline.sh` |
| Container restart liên tục | `docker logs <container>`; thường do thiếu RAM hoặc sai `hadoop.env` |
| Git Bash đổi `/opt/...` thành `C:/Program Files/Git/opt/...` | Chạy `export MSYS_NO_PATHCONV=1` trước lệnh `docker exec` |

## Tắt / dọn dẹp

```bash
docker compose -f hdfs/docker-compose.yml down       # tắt container, GIỮ dữ liệu HDFS + Hive metastore
docker compose -f hdfs/docker-compose.yml down -v    # XÓA toàn bộ dữ liệu (phải chạy lại pipeline)
```
