"""
B1 - Các tác vụ benchmark, cài đặt song song bằng pandas và PySpark.
Mỗi tác vụ đều bắt đầu từ việc đọc CSV (giống nhau giữa 2 engine).

Chạy một tác vụ pandas trong process riêng (được bench_pandas.py gọi):
    python3 tasks.py <task> <csv_path>   -> in ra JSON kết quả
"""
import json
import resource
import sys
import time

TASKS = ["read_count", "cleaning", "groupby_count", "label_profile"]
NON_NEGATIVE = ["flow_duration", "flow_bytes_s", "flow_packets_s"]

# (tên cột kết quả, cột nguồn, hàm) - dùng chung cho 2 engine
PROFILE = [
    ("avg_duration", "flow_duration", "mean"),
    ("median_duration", "flow_duration", "median"),   # Spark: percentile_approx
    ("avg_fwd_packets", "total_fwd_packets", "mean"),
    ("std_fwd_packets", "total_fwd_packets", "std"),
    ("avg_bytes_s", "flow_bytes_s", "mean"),
    ("avg_iat_mean", "flow_iat_mean", "mean"),
    ("max_packet_length", "max_packet_length", "max"),
]


# ---------------------------------------------------------------------------
# pandas
# ---------------------------------------------------------------------------
def pandas_task(task, path):
    import numpy as np
    import pandas as pd

    df = pd.read_csv(path, low_memory=False)
    if task == "read_count":
        return len(df)
    if task == "cleaning":
        num = df.select_dtypes(include="number").columns
        df[num] = df[num].replace([np.inf, -np.inf], np.nan)
        df = df.dropna()
        df = df[(df[NON_NEGATIVE] >= 0).all(axis=1)]
        df = df.drop_duplicates()
        return len(df)
    if task == "groupby_count":
        return len(df.groupby("label").size())
    if task == "label_profile":
        return len(df.groupby("label").agg(**{n: (c, fn) for n, c, fn in PROFILE}))
    raise ValueError(task)


# ---------------------------------------------------------------------------
# PySpark
# ---------------------------------------------------------------------------
def spark_read(spark, path):
    from schema import RAW_SCHEMA
    return (spark.read
            .option("header", True)
            .option("enforceSchema", True)
            .option("positiveInf", "Infinity")
            .option("negativeInf", "-Infinity")
            .schema(RAW_SCHEMA)
            .csv(path))


def spark_task(spark, task, path):
    from pyspark.sql import functions as F
    from schema import DOUBLE_COLUMNS

    df = spark_read(spark, path)
    if task == "read_count":
        return df.count()
    if task == "cleaning":
        for c in DOUBLE_COLUMNS:
            df = df.withColumn(c, F.when(F.isnan(c) | (F.abs(F.col(c)) == float("inf")), None)
                               .otherwise(F.col(c)))
        df = df.dropna()
        for c in NON_NEGATIVE:
            df = df.filter(F.col(c) >= 0)
        return df.dropDuplicates().count()
    if task == "groupby_count":
        return len(df.groupBy("label").count().collect())
    if task == "label_profile":
        funcs = {"mean": F.avg, "std": F.stddev, "max": F.max,
                 "median": lambda c: F.percentile_approx(c, 0.5)}
        return len(df.groupBy("label").agg(*[funcs[fn](c).alias(n) for n, c, fn in PROFILE]).collect())
    raise ValueError(task)


# ---------------------------------------------------------------------------
# Entry point cho 1 lần chạy pandas (process riêng -> đo peak memory chính xác)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    task, path = sys.argv[1], sys.argv[2]
    # Giới hạn RAM do process cha (bench_pandas.py) giám sát theo RSS thực tế.
    # Không dùng RLIMIT_AS: nó tính bộ nhớ ẢO -> pandas bị báo MemoryError dù RAM thật còn trống.
    # Nếu VM thật sự hết RAM, kernel ưu tiên kill process này thay vì các container khác.
    with open("/proc/self/oom_score_adj", "w") as f:
        f.write("1000")

    import numpy  # noqa: F401  (import trước khi bấm giờ)
    import pandas  # noqa: F401

    start = time.perf_counter()
    try:
        rows, status = pandas_task(task, path), "ok"
    except MemoryError:
        rows, status = None, "oom"
    seconds = round(time.perf_counter() - start, 3)
    peak_mb = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)
    print(json.dumps({"seconds": seconds, "rows": rows, "peak_mem_mb": peak_mb, "status": status}))
