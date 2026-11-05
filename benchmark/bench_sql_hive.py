"""
B2 + B3 (phần Hive) - chạy trên HOST (cần Docker CLI), gọi hive CLI trong container hive-server.

    python benchmark/bench_sql_hive.py [--queries ...] [--tables ...]

Thời gian lấy từ dòng "Time taken: X seconds, Fetched: N row(s)" của Hive
-> không tính thời gian khởi động CLI. Mỗi query chạy (1 warm-up + runs) lần trong cùng 1 session.
Hive chạy MapReduce trên YARN (xem hdfs/hadoop.env).
"""
import argparse
import os
import re
import subprocess
import tempfile

from common import now, row, save, parse_list
from sql_queries import QUERIES, TABLES, render

parser = argparse.ArgumentParser()
parser.add_argument("--queries", type=parse_list, default=list(QUERIES))
parser.add_argument("--tables", type=parse_list, default=TABLES)
parser.add_argument("--runs", type=int, default=3)
args = parser.parse_args()

TIME_RE = re.compile(r"Time taken: ([\d.]+) seconds, Fetched: (\d+) row")
# Không cho Hive trả lời COUNT(*) từ statistics trong metastore (bảng do Spark tạo không có stats)
SETTINGS = "SET hive.compute.query.using.stats=false;\n"

rows = []
for table in args.tables:
    for query in args.queries:
        sql = render(query, table)
        script = SETTINGS + "".join(f"{sql};\n" for _ in range(args.runs + 1))
        with tempfile.NamedTemporaryFile("w", suffix=".hql", delete=False, encoding="utf-8") as f:
            f.write(script)
        started = now()
        subprocess.run(["docker", "cp", f.name, "hive-server:/tmp/bench.hql"], check=True)
        os.remove(f.name)
        proc = subprocess.run(["docker", "exec", "hive-server", "hive", "-f", "/tmp/bench.hql"],
                              capture_output=True, text=True, encoding="utf-8", errors="replace")
        output = proc.stdout + proc.stderr
        timings = TIME_RE.findall(output)

        for run in range(0, args.runs + 1):
            if run < len(timings):
                seconds, n = timings[run]
                status = "ok"
            else:
                seconds, n = None, None
                failed = [l for l in output.splitlines() if "FAILED" in l or "Error" in l]
                status = f"error: {failed[0][:100] if failed else 'no timing'}"
            rows.append(row(benchmark="sql_engine", engine="hive", task=query, table=table,
                            data_pct=100, config="mapreduce-on-yarn", run=run, seconds=seconds,
                            rows=n, status=status, started_at=started))
            print(f"hive {table:<9} {query:<16} run {run}: {seconds}s rows={n} {status}")

save("b2b3_sql_hive", rows)
