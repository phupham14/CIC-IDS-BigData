"""Upload CSV gốc từ data/raw_data lên HDFS: /cic-ids2017/raw/<day>/<file>.csv"""
import os
import re

from config import get_spark, LOCAL_RAW_DIR, RAW_DIR

DAYS = ["monday", "tuesday", "wednesday", "thursday", "friday"]

spark = get_spark("ingest-raw")
jvm = spark._jvm
hadoop_conf = spark._jsc.hadoopConfiguration()
fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(RAW_DIR), hadoop_conf)
Path = jvm.org.apache.hadoop.fs.Path

for file_name in sorted(os.listdir(LOCAL_RAW_DIR)):
    if not file_name.endswith(".csv"):
        continue
    match = re.match(r"(?i)(" + "|".join(DAYS) + ")", file_name)
    if not match:
        print(f"[SKIP] Không xác định được ngày: {file_name}")
        continue

    day = match.group(1).lower()
    target = Path(f"{RAW_DIR}/{day}/{file_name}")
    # overwrite=True -> chạy lại nhiều lần vẫn an toàn
    fs.copyFromLocalFile(False, True, Path(f"file://{LOCAL_RAW_DIR}/{file_name}"), target)
    size_mb = fs.getFileStatus(target).getLen() / 1024 / 1024
    print(f"[OK] {file_name} -> {target.toString()} ({size_mb:.1f} MB)")

spark.stop()
