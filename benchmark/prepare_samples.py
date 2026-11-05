"""
Tạo các bộ dữ liệu benchmark 10% .. 400% từ CSV gốc trên HDFS.

  - <= 100%: lấy mẫu ngẫu nhiên (seed cố định)
  - >  100%: nhân bản dataset gốc k lần (200% = 2 bản, 400% = 4 bản)

Mỗi bộ được ghi 2 nơi với nội dung giống hệt nhau:
  - HDFS  /cic-ids2017/benchmark/raw_pXXX/   -> Spark đọc
  - local /bench_local/raw_pXXX.csv          -> pandas đọc (docker volume, không qua bind mount Windows)
"""
import argparse
import csv
import os
from functools import reduce

from pyspark.sql import DataFrame

from common import SIZES, RESULTS_DIR, LOCAL_SAMPLE_DIR, local_sample, hdfs_sample, parse_list
from config import get_spark, RAW_DIR
from schema import RAW_SCHEMA

parser = argparse.ArgumentParser()
parser.add_argument("--sizes", type=lambda v: parse_list(v, int), default=SIZES)
args = parser.parse_args()

spark = get_spark("benchmark-prepare")
jvm = spark._jvm
Path = jvm.org.apache.hadoop.fs.Path
fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(RAW_DIR), spark._jsc.hadoopConfiguration())

raw = (spark.read.option("recursiveFileLookup", True)
       .option("header", True).option("enforceSchema", True)
       .option("positiveInf", "Infinity").option("negativeInf", "-Infinity")
       .schema(RAW_SCHEMA).csv(RAW_DIR))

os.makedirs(LOCAL_SAMPLE_DIR, exist_ok=True)
meta = []
for pct in args.sizes:
    if pct <= 100:
        df = raw if pct == 100 else raw.sample(False, pct / 100, seed=42)
    else:
        df = reduce(DataFrame.unionAll, [raw] * (pct // 100))

    target = hdfs_sample(pct)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(target)

    part = [s.getPath() for s in fs.listStatus(Path(target)) if s.getPath().getName().startswith("part-")][0]
    local = local_sample(pct)
    if os.path.exists(local):
        os.remove(local)
    fs.copyToLocalFile(False, part, Path(f"file://{local}"), True)   # True: không tạo file .crc

    rows = spark.read.option("header", True).csv(target).count()
    size_mb = round(os.path.getsize(local) / 1024 / 1024, 1)
    meta.append({"data_pct": pct, "rows": rows, "size_mb": size_mb})
    print(f"[{pct:>3}%] rows={rows:,} size={size_mb} MB -> {target} + {local}")

os.makedirs(RESULTS_DIR, exist_ok=True)
with open(os.path.join(RESULTS_DIR, "samples.csv"), "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["data_pct", "rows", "size_mb"])
    writer.writeheader()
    writer.writerows(meta)
spark.stop()
