#!/usr/bin/env bash
# Chạy toàn bộ benchmark (cluster phải đang chạy và pipeline đã chạy xong - xem run_pipeline.sh).
#
#   ./benchmark/run_benchmark.sh                  # tất cả (~2-3 giờ)
#   ./benchmark/run_benchmark.sh --only sql_hive  # chỉ một bước
#   ./benchmark/run_benchmark.sh --from b4        # từ một bước trở đi
#
# Các bước: prepare | b1_pandas | b1_spark | b4 | sql_spark | sql_hive | summarize
# Trong lúc chạy, CPU/RAM của các container được ghi vào results/docker_stats.csv
set -euo pipefail
export MSYS_NO_PATHCONV=1

cd "$(dirname "${BASH_SOURCE[0]}")/.."
PYTHON="${PYTHON:-python}"
RESULTS=benchmark/results
STEPS=(prepare b1_pandas b1_spark b4 sql_spark sql_hive summarize)
SCALABILITY_CORES=(1 2 4)
SCALABILITY_SIZES="100,200"

FROM="prepare"; ONLY=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --from) FROM="$2"; shift 2 ;;
        --only) ONLY="$2"; shift 2 ;;
        -h|--help) sed -n '2,9p' "$0"; exit 0 ;;
        *) echo "Tham số không hợp lệ: $1"; exit 1 ;;
    esac
done
[[ " ${STEPS[*]} " == *" ${ONLY:-$FROM} "* ]] || { echo "Bước không tồn tại (có: ${STEPS[*]})"; exit 1; }

log() { echo -e "\n\033[1;34m==> [$(date +%H:%M:%S)] $*\033[0m"; }
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
submit() { docker exec jupyter spark-submit "$@" 2>&1 | grep -vE " (INFO|WARN) |^\s*$"; }

# --- Ghi CPU/RAM các container mỗi ~5 giây (resource utilization) ---
mkdir -p "$RESULTS"
STATS="$RESULTS/docker_stats.csv"
[[ -f "$STATS" ]] || echo "timestamp,container,cpu_pct,mem_usage" > "$STATS"
(
    while true; do
        ts=$(date +%Y-%m-%dT%H:%M:%S)
        docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' 2>/dev/null \
            | sed "s|^|$ts,|; s|%||" >> "$STATS"
        sleep 3
    done
) &
SAMPLER=$!
trap 'kill $SAMPLER 2>/dev/null || true' EXIT

START=$(date +%s)

if should_run prepare; then
    log "Chuẩn bị dữ liệu 10%..400% (HDFS + local volume)"
    submit /opt/project/benchmark/prepare_samples.py
fi

if should_run b1_pandas; then
    log "B1 - pandas (mỗi lần đo là 1 process, giới hạn 10 GB RAM)"
    docker exec jupyter python3 /opt/project/benchmark/bench_pandas.py
fi

if should_run b1_spark; then
    log "B1 - PySpark (2 executor x 2 core x 2 GB)"
    submit /opt/project/benchmark/bench_spark_tasks.py
fi

if should_run b4; then
    for n in "${SCALABILITY_CORES[@]}"; do
        log "B4 - Scalability: $n core (executor 1 core x 1 GB)"
        submit --conf spark.cores.max="$n" --conf spark.executor.cores=1 --conf spark.executor.memory=1g \
            /opt/project/benchmark/bench_spark_tasks.py --benchmark scalability --config "cores=$n" \
            --sizes "$SCALABILITY_SIZES" --tasks cleaning,label_profile
    done
fi

if should_run sql_spark; then
    log "B2 + B3 - Spark SQL trên flows (Parquet) và flows_csv (CSV)"
    submit /opt/project/benchmark/bench_sql_spark.py
fi

if should_run sql_hive; then
    log "B2 + B3 - Hive (MapReduce on YARN) trên flows và flows_csv"
    "$PYTHON" benchmark/bench_sql_hive.py
fi

if should_run summarize; then
    log "Tổng hợp kết quả"
    "$PYTHON" benchmark/summarize.py
fi

log "Hoàn tất sau $(( ($(date +%s) - START) / 60 )) phút. Kết quả: $RESULTS/"
