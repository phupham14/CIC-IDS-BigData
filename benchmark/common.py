"""Tiện ích dùng chung: ghi kết quả benchmark (chạy được cả trong container lẫn trên host)."""
import csv
import os
from datetime import datetime

RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
SIZES = [10, 25, 50, 75, 100, 200, 400]          # % so với dataset gốc
LOCAL_SAMPLE_DIR = "/bench_local"                 # docker volume trong container jupyter
HDFS_SAMPLE_DIR = "hdfs://namenode:9000/cic-ids2017/benchmark"

FIELDS = ["benchmark", "engine", "task", "table", "data_pct", "config",
          "run", "seconds", "rows", "peak_mem_mb", "status", "started_at"]
KEY = ("benchmark", "engine", "task", "table", "data_pct", "config")


def local_sample(pct):
    return f"{LOCAL_SAMPLE_DIR}/raw_p{pct:03d}.csv"


def hdfs_sample(pct):
    return f"{HDFS_SAMPLE_DIR}/raw_p{pct:03d}"


def now():
    return datetime.now().isoformat(timespec="seconds")


def row(**kw):
    r = {f: "" for f in FIELDS}
    r.update({k: v for k, v in kw.items() if v is not None})
    return r


def save(name, rows):
    """Ghi vào results/<name>.csv. Chạy lại cùng cấu hình thì thay thế kết quả cũ của cấu hình đó."""
    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, f"{name}.csv")
    existing = []
    if os.path.exists(path):
        with open(path, newline="", encoding="utf-8") as f:
            existing = list(csv.DictReader(f))
    new_keys = {tuple(str(r[k]) for k in KEY) for r in rows}
    kept = [r for r in existing if tuple(r[k] for k in KEY) not in new_keys]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(kept + rows)
    print(f"Saved {len(rows)} rows -> {path}")


def parse_list(value, cast=str):
    return [cast(v) for v in value.split(",")] if value else None
