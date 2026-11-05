"""
B1 (phần pandas) - chạy trong container jupyter bằng python3 (KHÔNG mở SparkSession).

Mỗi lần đo là một process riêng (tasks.py) -> đo peak memory chính xác. Process cha giám sát
RSS (RAM thật) của process con và kill nếu vượt --mem-limit-gb -> ghi nhận 'oom'
thay vì để VM hết RAM làm sập các container khác.
"""
import argparse
import json
import os
import subprocess
import sys
import time

from common import SIZES, local_sample, now, row, save, parse_list
from tasks import TASKS

parser = argparse.ArgumentParser()
parser.add_argument("--sizes", type=lambda v: parse_list(v, int), default=SIZES)
parser.add_argument("--tasks", type=parse_list, default=TASKS)
parser.add_argument("--runs", type=int, default=3)
parser.add_argument("--mem-limit-gb", type=float, default=10)
parser.add_argument("--timeout", type=int, default=1800)
args = parser.parse_args()

TASKS_PY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tasks.py")
config = f"mem_limit={args.mem_limit_gb:g}GB"


def rss_mb(pid):
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except FileNotFoundError:
        pass
    return 0


def run_once(task, path):
    limit_mb = args.mem_limit_gb * 1024
    start, peak, status = time.time(), 0, None
    proc = subprocess.Popen([sys.executable, TASKS_PY, task, path],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    while proc.poll() is None:
        peak = max(peak, rss_mb(proc.pid))
        if peak > limit_mb:
            status = "oom"          # vượt ngân sách RAM của "máy đơn"
        elif time.time() - start > args.timeout:
            status = "timeout"
        if status:
            proc.kill()
            proc.wait()
            return {"status": status, "seconds": round(time.time() - start, 3),
                    "peak_mem_mb": round(peak, 1)}
        time.sleep(0.1)

    out, err = proc.communicate()
    if proc.returncode != 0:
        # bị kernel OOM killer kill (-9) hoặc MemoryError ngoài khối try
        status = "oom" if "MemoryError" in err or proc.returncode < 0 else "error"
        print(err[-500:])
        return {"status": status, "peak_mem_mb": round(peak, 1)}
    return json.loads(out.strip().splitlines()[-1])


failed = {}   # task -> % nhỏ nhất bị lỗi -> bỏ qua các mức lớn hơn
for pct in args.sizes:
    path = local_sample(pct)
    if not os.path.exists(path):
        sys.exit(f"Thiếu {path} - chạy prepare_samples.py trước")
    rows = []

    # Warm-up (run=0): nạp file vào page cache, không tính vào kết quả
    if not failed:
        warm = run_once("read_count", path)
        rows.append(row(benchmark="pandas_vs_spark", engine="pandas", task="read_count",
                        data_pct=pct, config=config, run=0, started_at=now(), **warm))

    for task in args.tasks:
        if task in failed:
            rows.append(row(benchmark="pandas_vs_spark", engine="pandas", task=task, data_pct=pct,
                            config=config, run=1, status=f"skipped(failed at {failed[task]}%)"))
            continue
        for run in range(1, args.runs + 1):
            started = now()
            result = run_once(task, path)
            rows.append(row(benchmark="pandas_vs_spark", engine="pandas", task=task, data_pct=pct,
                            config=config, run=run, started_at=started, **result))
            print(f"pandas {task:<14} {pct:>3}% run {run}: {result}")
            if result["status"] != "ok":
                failed[task] = pct
                break
    save("b1_pandas", rows)
