#!/usr/bin/env bash
# Dựng lại toàn bộ pipeline CIC-IDS2017 bằng một lệnh.
#
#   ./run_pipeline.sh                 # chạy tất cả các bước
#   ./run_pipeline.sh --from spark    # bắt đầu từ một bước (cluster|pandas|hdfs|hive|spark|analytics)
#   ./run_pipeline.sh --only hive     # chỉ chạy một bước
#
# Chạy trên host (Git Bash / Linux / macOS), yêu cầu Docker + Python (pandas, numpy).
# Mọi bước đều idempotent: chạy lại nhiều lần không làm trùng dữ liệu.
set -euo pipefail

# Git Bash trên Windows tự đổi "/path" thành "C:/..." khi gọi docker exec -> tắt đi
export MSYS_NO_PATHCONV=1

# -> đường dẫn host luôn dùng dạng tương đối (so với thư mục gốc repo)
cd "$(dirname "${BASH_SOURCE[0]}")"
COMPOSE="docker compose -f hdfs/docker-compose.yml"
PYTHON="${PYTHON:-python}"
STEPS=(cluster pandas hdfs hive spark analytics)

FROM="cluster"
ONLY=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --from) FROM="$2"; shift 2 ;;
        --only) ONLY="$2"; shift 2 ;;
        -h|--help) sed -n '2,9p' "$0"; exit 0 ;;
        *) echo "Tham số không hợp lệ: $1"; exit 1 ;;
    esac
done

log()  { echo -e "\n\033[1;34m==> $*\033[0m"; }
fail() { echo -e "\033[1;31m[ERROR] $*\033[0m" >&2; exit 1; }

should_run() {
    local step="$1"
    if [[ -n "$ONLY" ]]; then [[ "$step" == "$ONLY" ]]; return; fi
    local started=0
    for s in "${STEPS[@]}"; do
        [[ "$s" == "$FROM" ]] && started=1
        [[ "$s" == "$step" ]] && { [[ $started == 1 ]]; return; }
    done
    return 1
}

wait_for() {  # wait_for "<mô tả>" <timeout giây> <lệnh...>
    local desc="$1" timeout="$2"; shift 2
    local waited=0
    until "$@" >/dev/null 2>&1; do
        (( waited >= timeout )) && fail "Hết thời gian chờ: $desc"
        sleep 5; waited=$((waited + 5))
    done
    echo "    $desc: OK (${waited}s)"
}

spark_submit() {
    log "Spark: $1"
    set +e
    docker exec jupyter spark-submit "/opt/project/spark/$1" 2>&1 \
        | grep -vE " (INFO|WARN) |^\s+at |^\s*$"
    local rc=${PIPESTATUS[0]}   # exit code của spark-submit, không phải của grep
    set -e
    [[ $rc -eq 0 ]] || fail "$1 thất bại (exit $rc)"
}

[[ " ${STEPS[*]} " == *" ${ONLY:-$FROM} "* ]] || fail "Bước không tồn tại: ${ONLY:-$FROM} (có: ${STEPS[*]})"

START=$(date +%s)

# ---------------------------------------------------------------------------
# 1. Khởi động cluster và chờ các service sẵn sàng
# ---------------------------------------------------------------------------
if should_run cluster; then
    log "Khởi động cluster (Hadoop + Hive + Spark)"
    if [[ ! -f hdfs/hadoop.env ]]; then
        cp hdfs/hadoop.env.example hdfs/hadoop.env
        echo "    Đã tạo hdfs/hadoop.env từ hadoop.env.example"
    fi
    $COMPOSE build --quiet spark-master
    $COMPOSE up -d

    wait_for "HDFS ra khỏi safe mode" 180 docker exec namenode hdfs dfsadmin -safemode wait
    wait_for "DataNode đã đăng ký" 180 \
        bash -c "docker exec namenode hdfs dfsadmin -report 2>/dev/null | grep -qE 'Live datanodes \([1-9]'"
    wait_for "Hive metastore (9083)" 300 \
        docker exec hive-server bash -c "timeout 2 bash -c '</dev/tcp/hive-metastore/9083'"
    wait_for "Spark có 2 worker" 180 docker exec jupyter python3 -c \
        "import json,urllib.request as u; assert json.load(u.urlopen('http://spark-master:8080/json/'))['aliveworkers'] >= 2"
fi

# ---------------------------------------------------------------------------
# 2. Làm sạch cơ bản bằng pandas (Thành viên 1) -> data/cleaned_data
# ---------------------------------------------------------------------------
if should_run pandas; then
    log "Pandas cleaning (data/data_cleaning.py)"
    ls data/raw_data/*.csv >/dev/null 2>&1 || fail "Không có CSV trong data/raw_data/"
    if ls data/cleaned_data/*.csv >/dev/null 2>&1; then
        echo "    data/cleaned_data đã có dữ liệu -> bỏ qua (xóa thư mục để chạy lại)"
    else
        "$PYTHON" "data/data_cleaning.py"
    fi
fi

# ---------------------------------------------------------------------------
# 3. HDFS: tạo cấu trúc thư mục + upload dữ liệu đã làm sạch
# ---------------------------------------------------------------------------
if should_run hdfs; then
    log "HDFS: tạo thư mục và upload cleaned CSV"
    docker exec namenode rm -rf /cleaned_data /hdfs_setup.sh
    docker cp "data/cleaned_data" namenode:/cleaned_data
    docker cp "hdfs/hdfs_setup.sh" namenode:/hdfs_setup.sh
    docker exec namenode bash -c "
        sed -i 's/\r$//' /hdfs_setup.sh && bash /hdfs_setup.sh >/dev/null 2>&1 &&
        hdfs dfs -put -f /cleaned_data/*.csv /cic-ids2017/cleaned/ 2>/dev/null &&
        hdfs dfs -count -h /cic-ids2017/cleaned"
fi

# ---------------------------------------------------------------------------
# 4. Hive: tạo bảng raw + partitioned (hive/create_tables.hql)
# ---------------------------------------------------------------------------
if should_run hive; then
    log "Hive: create_tables.hql (khoảng 1-2 phút)"
    docker cp "hive/create_tables.hql" hive-server:/create_tables.hql
    docker cp "hive/queries.hql" hive-server:/queries.hql
    HIVE_LOG="spark/reports/hive_create_tables.log"
    mkdir -p "$(dirname "$HIVE_LOG")"
    docker exec hive-server hive -f /create_tables.hql > "$HIVE_LOG" 2>&1 \
        || { tail -20 "$HIVE_LOG"; fail "create_tables.hql thất bại (log: $HIVE_LOG)"; }
    grep -E "SUCCESS|Time taken: [0-9.]+ seconds$" "$HIVE_LOG" | tail -2
fi

# ---------------------------------------------------------------------------
# 5. Spark: ingest raw -> cleaning -> transform
# ---------------------------------------------------------------------------
if should_run spark; then
    spark_submit ingest_raw.py
    spark_submit cleaning.py
    spark_submit transform.py
fi

# ---------------------------------------------------------------------------
# 6. Spark SQL analytics -> processed/analytics + spark/reports/analytics/*.csv
# ---------------------------------------------------------------------------
if should_run analytics; then
    spark_submit analytics.py
fi

log "Hoàn tất sau $(( $(date +%s) - START ))s"
cat <<EOF
    NameNode UI      http://localhost:9870
    Spark master UI  http://localhost:8080
    JupyterLab       http://localhost:8888
    Reports          spark/reports/*.json
EOF
