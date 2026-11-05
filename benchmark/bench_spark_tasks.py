"""
B1 (phần PySpark) và B4 (scalability) - chạy bằng spark-submit.

  B1: spark-submit bench_spark_tasks.py
  B4: spark-submit --conf spark.cores.max=2 --conf spark.executor.cores=1 --conf spark.executor.memory=1g \
          bench_spark_tasks.py --benchmark scalability --config cores=2 --sizes 100,200 --tasks cleaning
"""
import argparse
import time

from common import SIZES, hdfs_sample, now, row, save, parse_list
from config import get_spark
from tasks import TASKS, spark_task

parser = argparse.ArgumentParser()
parser.add_argument("--benchmark", default="pandas_vs_spark", choices=["pandas_vs_spark", "scalability"])
parser.add_argument("--config", default="default")
parser.add_argument("--sizes", type=lambda v: parse_list(v, int), default=SIZES)
parser.add_argument("--tasks", type=parse_list, default=TASKS)
parser.add_argument("--runs", type=int, default=3)
args = parser.parse_args()

t = time.perf_counter()
spark = get_spark(f"benchmark-{args.benchmark}-{args.config}")
spark.sparkContext.setLogLevel("ERROR")
startup = round(time.perf_counter() - t, 3)
config = args.config
conf = spark.sparkContext.getConf()
print(f"SparkSession startup {startup}s | config={config} | cores.max={conf.get('spark.cores.max', 'all')} "
      f"executor.cores={conf.get('spark.executor.cores')} executor.memory={conf.get('spark.executor.memory')}")

rows = [row(benchmark=args.benchmark, engine="spark", task="session_startup", data_pct="",
            config=config, run=1, seconds=startup, status="ok", started_at=now())]

for pct in args.sizes:
    path = hdfs_sample(pct)
    # Warm-up (run=0): JIT của JVM + page cache trên DataNode
    t = time.perf_counter()
    spark_task(spark, "read_count", path)
    rows.append(row(benchmark=args.benchmark, engine="spark", task="read_count", data_pct=pct,
                    config=config, run=0, seconds=round(time.perf_counter() - t, 3), status="ok"))

    for task in args.tasks:
        for run in range(1, args.runs + 1):
            started = now()
            t = time.perf_counter()
            try:
                result, status = spark_task(spark, task, path), "ok"
            except Exception as e:      # ghi nhận lỗi thay vì dừng cả benchmark
                result, status = None, f"error: {str(e).splitlines()[0][:100]}"
            seconds = round(time.perf_counter() - t, 3)
            rows.append(row(benchmark=args.benchmark, engine="spark", task=task, data_pct=pct,
                            config=config, run=run, seconds=seconds, rows=result,
                            status=status, started_at=started))
            print(f"spark {task:<14} {pct:>3}% run {run}: {seconds}s rows={result} {status}")

save("b1_spark" if args.benchmark == "pandas_vs_spark" else "b4_scalability", rows)
spark.stop()
