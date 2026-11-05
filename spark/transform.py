"""
Giai đoạn 3 - Transformation & Feature Engineering bằng PySpark.

Input : hdfs:///cic-ids2017/processed/cleaned_spark/   (output của cleaning.py)
Output: hdfs:///cic-ids2017/processed/flows/           (Parquet, partition theo attack_category)
        hdfs:///cic-ids2017/processed/flows_csv/       (cùng dữ liệu dạng CSV, cho benchmark CSV vs Parquet)
        hdfs:///cic-ids2017/processed/dim_*/           (bảng dimension)
        Hive tables: cic_ids2017.flows, flows_csv, dim_port_service, dim_attack_category
        spark/reports/transform_report.json
"""
import json
import os
import time

from pyspark.sql import functions as F

from config import (get_spark, SPARK_CLEANED_DIR, FLOWS_PARQUET_DIR, FLOWS_CSV_DIR,
                    DIM_PORT_SERVICE_DIR, DIM_ATTACK_CATEGORY_DIR, HIVE_DB, REPORT_DIR)
from dimensions import port_service_df, attack_category_df, SESSION_WINDOWS, FILE_WINDOWS
from schema import FEATURE_COLUMNS

spark = get_spark("transform")
start = time.time()
report = {}

df = spark.read.parquet(SPARK_CLEANED_DIR)

# ---------------------------------------------------------------------------
# 1. Loại cột hằng số (cùng một giá trị trên toàn dataset -> không mang thông tin)
# ---------------------------------------------------------------------------
numeric_cols = [c for c in FEATURE_COLUMNS if c in df.columns]
stats = df.agg(*[F.min(c).alias(f"min__{c}") for c in numeric_cols],
               *[F.max(c).alias(f"max__{c}") for c in numeric_cols]).first()
constant_cols = [c for c in numeric_cols if stats[f"min__{c}"] == stats[f"max__{c}"]]
df = df.drop(*constant_cols)
report["dropped_constant_columns"] = constant_cols
print(f"Dropped {len(constant_cols)} constant columns: {constant_cols}")

# ---------------------------------------------------------------------------
# 2. Feature thời gian giả lập từ seq_index (xem dimensions.SESSION_WINDOWS)
# ---------------------------------------------------------------------------
def window_lookup(windows, key_col, fn):
    return F.create_map(*[F.lit(x) for k, w in windows.items() for x in (k, fn(*w))])[F.col(key_col)]


position = F.col("seq_index") / F.col("file_rows")          # 0.0 -> 1.0 trong file
# Ưu tiên khung giờ riêng của file (FILE_WINDOWS), nếu không có thì theo session
window_start = F.coalesce(window_lookup(FILE_WINDOWS, "source_file", lambda s, e: s),
                          window_lookup(SESSION_WINDOWS, "session", lambda s, e: s))
window_len = F.coalesce(window_lookup(FILE_WINDOWS, "source_file", lambda s, e: e - s),
                        window_lookup(SESSION_WINDOWS, "session", lambda s, e: e - s))
pseudo_minute = F.floor(window_start + position * window_len)

df = (df
      .withColumn("time_bucket", F.floor(position * 100).cast("int"))   # 0..99
      .withColumn("pseudo_minute", pseudo_minute.cast("int"))
      .withColumn("pseudo_hour", F.floor(F.col("pseudo_minute") / 60).cast("int"))
      .withColumn("pseudo_time", F.format_string("%02d:%02d",
                                                 F.col("pseudo_hour"), F.col("pseudo_minute") % 60)))

# ---------------------------------------------------------------------------
# 3. Feature mạng: dải port + service (JOIN với bảng dimension)
# ---------------------------------------------------------------------------
df = (df
      .withColumn("destination_port", F.col("destination_port").cast("int"))
      .withColumn("port_range",
                  F.when(F.col("destination_port") <= 1023, "well_known")
                   .when(F.col("destination_port") <= 49151, "registered")
                   .otherwise("dynamic")))

dim_port = port_service_df(spark)
df = (df.join(F.broadcast(dim_port), on="destination_port", how="left")
        .withColumn("service", F.coalesce("service", F.lit("unknown")))
        .withColumn("service_group", F.coalesce("service_group", F.lit("unknown"))))

# ---------------------------------------------------------------------------
# 4. Feature lưu lượng
# ---------------------------------------------------------------------------
total_packets = F.col("total_fwd_packets") + F.col("total_backward_packets")
total_bytes = F.col("total_length_of_fwd_packets") + F.col("total_length_of_bwd_packets")

df = (df
      .withColumn("flow_duration_s", F.col("flow_duration") / 1e6)          # micro giây -> giây
      .withColumn("total_packets", total_packets)
      .withColumn("total_bytes", total_bytes)
      .withColumn("bytes_per_packet",
                  F.when(total_packets > 0, total_bytes / total_packets))
      .withColumn("fwd_bwd_packet_ratio",
                  F.when(F.col("total_backward_packets") > 0,
                         F.col("total_fwd_packets") / F.col("total_backward_packets")))
      .withColumn("fwd_bytes_share",
                  F.when(total_bytes > 0, F.col("total_length_of_fwd_packets") / total_bytes))
      .withColumn("has_response", (F.col("total_backward_packets") > 0).cast("int")))

df = df.drop("file_rows")

# ---------------------------------------------------------------------------
# 5. Ghi Parquet + CSV (cùng dữ liệu, cùng layout) partition theo attack_category
# ---------------------------------------------------------------------------
df = df.repartition("attack_category", "day").persist()
report["rows"] = df.count()
report["category_counts"] = {r["attack_category"]: r["count"] for r in
                             df.groupBy("attack_category").count().orderBy(F.desc("count")).collect()}

t = time.time()
df.write.mode("overwrite").partitionBy("attack_category").parquet(FLOWS_PARQUET_DIR)
report["write_seconds_parquet"] = round(time.time() - t, 1)

t = time.time()
# Không ghi header: Spark đọc Hive text table KHÔNG tôn trọng skip.header.line.count
# -> dòng header bị đọc thành dữ liệu. Schema đã nằm trong định nghĩa Hive table.
df.write.mode("overwrite").partitionBy("attack_category").option("header", False).csv(FLOWS_CSV_DIR)
report["write_seconds_csv"] = round(time.time() - t, 1)

port_service_df(spark).write.mode("overwrite").parquet(DIM_PORT_SERVICE_DIR)
attack_category_df(spark).write.mode("overwrite").parquet(DIM_ATTACK_CATEGORY_DIR)

# ---------------------------------------------------------------------------
# 6. Đăng ký Hive external tables (đọc được từ cả Hive lẫn Spark SQL)
# ---------------------------------------------------------------------------
data_cols = [f for f in df.schema.fields if f.name != "attack_category"]
columns_ddl = ",\n    ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in data_cols)

spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB}")
spark.sql(f"DROP TABLE IF EXISTS {HIVE_DB}.flows")
spark.sql(f"""
CREATE EXTERNAL TABLE {HIVE_DB}.flows (
    {columns_ddl}
)
PARTITIONED BY (attack_category STRING)
STORED AS PARQUET
LOCATION '{FLOWS_PARQUET_DIR}'
""")
spark.sql(f"DROP TABLE IF EXISTS {HIVE_DB}.flows_csv")
spark.sql(f"""
CREATE EXTERNAL TABLE {HIVE_DB}.flows_csv (
    {columns_ddl}
)
PARTITIONED BY (attack_category STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '{FLOWS_CSV_DIR}'
""")
for table in ("flows", "flows_csv"):
    spark.sql(f"MSCK REPAIR TABLE {HIVE_DB}.{table}")

for table, path, schema in (("dim_port_service", DIM_PORT_SERVICE_DIR, port_service_df(spark).schema),
                            ("dim_attack_category", DIM_ATTACK_CATEGORY_DIR, attack_category_df(spark).schema)):
    ddl = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in schema.fields)
    spark.sql(f"DROP TABLE IF EXISTS {HIVE_DB}.{table}")
    spark.sql(f"CREATE EXTERNAL TABLE {HIVE_DB}.{table} ({ddl}) STORED AS PARQUET LOCATION '{path}'")

# ---------------------------------------------------------------------------
# 7. Report: kích thước lưu trữ trên HDFS
# ---------------------------------------------------------------------------
jvm = spark._jvm
fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(FLOWS_PARQUET_DIR),
                                             spark._jsc.hadoopConfiguration())
for name, path in (("parquet", FLOWS_PARQUET_DIR), ("csv", FLOWS_CSV_DIR)):
    size = fs.getContentSummary(jvm.org.apache.hadoop.fs.Path(path)).getLength()
    report[f"size_mb_{name}"] = round(size / 1024 / 1024, 1)

report["columns"] = [f"{f.name}:{f.dataType.simpleString()}" for f in df.schema.fields]
report["elapsed_seconds"] = round(time.time() - start, 1)

os.makedirs(REPORT_DIR, exist_ok=True)
with open(os.path.join(REPORT_DIR, "transform_report.json"), "w", encoding="utf-8") as f:
    json.dump(report, f, indent=2, ensure_ascii=False)

print(json.dumps({k: v for k, v in report.items() if k != "columns"}, indent=2, ensure_ascii=False))
spark.table(f"{HIVE_DB}.flows").select(
    "day", "session", "seq_index", "time_bucket", "pseudo_time", "destination_port",
    "service", "port_range", "bytes_per_packet", "fwd_bwd_packet_ratio", "label", "attack_category"
).show(5, truncate=False)
spark.stop()
