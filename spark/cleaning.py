"""
Giai đoạn 2 - Data Cleaning bằng PySpark.

Input : hdfs:///cic-ids2017/raw/<day>/*.csv          (CSV gốc, 79 cột)
Output: hdfs:///cic-ids2017/processed/cleaned_spark/  (Parquet, partition theo day)
        spark/reports/cleaning_report.json           (số dòng qua từng bước)
"""
import json
import os
import time

from pyspark.sql import Window, functions as F

from config import get_spark, RAW_DIR, SPARK_CLEANED_DIR, REPORT_DIR
from schema import (RAW_SCHEMA, COLUMNS, FEATURE_COLUMNS, DOUBLE_COLUMNS,
                    LABEL_TO_CATEGORY, normalize_header)

spark = get_spark("cleaning")
# Mỗi file CSV nằm trọn trong 1 split -> giữ đúng thứ tự dòng khi đánh seq_index
spark.conf.set("spark.sql.files.maxPartitionBytes", str(1024 * 1024 * 1024))

report = {"steps": []}
start = time.time()


def log_step(name, df):
    count = df.count()
    prev = report["steps"][-1]["rows"] if report["steps"] else count
    report["steps"].append({"step": name, "rows": count, "removed": prev - count})
    print(f"[{name}] rows = {count:,} (removed {prev - count:,})")


# ---------------------------------------------------------------------------
# 1. Kiểm tra header của từng file khớp với schema tường minh
# ---------------------------------------------------------------------------
expected = [name for name, _ in COLUMNS]
files = [r.path for r in spark.read.format("binaryFile")
         .option("recursiveFileLookup", True).load(RAW_DIR).select("path").collect()
         if r.path.endswith(".csv")]

for path in files:
    header = spark.read.text(path).first()[0].split(",")
    names = [normalize_header(h) for h in header]
    # Cột "Fwd Header Length" xuất hiện 2 lần trong file gốc
    names[names.index("fwd_header_length", 40)] = "fwd_header_length_1"
    if names != expected:
        diff = [(i, a, b) for i, (a, b) in enumerate(zip(names, expected)) if a != b]
        raise ValueError(f"Header không khớp schema: {path}\n{diff[:5]}")
print(f"Header OK cho {len(files)} file")

# ---------------------------------------------------------------------------
# 2. Đọc CSV với schema tường minh + metadata nguồn (day, session)
# ---------------------------------------------------------------------------
df = (spark.read
      .option("header", True)
      .option("enforceSchema", True)
      .option("mode", "PERMISSIVE")
      .option("ignoreLeadingWhiteSpace", True)
      .option("positiveInf", "Infinity")
      .option("negativeInf", "-Infinity")
      .option("recursiveFileLookup", True)
      .schema(RAW_SCHEMA)
      .csv(RAW_DIR))

df = (df
      .withColumn("source_file", F.regexp_extract(F.input_file_name(), r"([^/]+)\.csv$", 1))
      .withColumn("day", F.regexp_extract(F.input_file_name(), r"/raw/([a-z]+)/", 1))
      .withColumn("session", F.lower(F.regexp_extract("source_file", r"(?i)(morning|afternoon)", 1)))
      .withColumn("session", F.when(F.col("session") == "", "fullday").otherwise(F.col("session"))))

# ---------------------------------------------------------------------------
# 3. seq_index: vị trí dòng trong file gốc (dùng làm thời gian tương đối)
#    Phải đánh TRƯỚC khi lọc dòng để giữ đúng vị trí ban đầu.
# ---------------------------------------------------------------------------
df = (df
      .withColumn("_mid", F.monotonically_increasing_id())
      .withColumn("seq_index",
                  F.row_number().over(Window.partitionBy("source_file").orderBy("_mid")) - 1)
      .withColumn("file_rows", F.count(F.lit(1)).over(Window.partitionBy("source_file")))
      .drop("_mid"))

df = df.persist()
log_step("raw", df)

# ---------------------------------------------------------------------------
# 4. Chuẩn hóa label: sửa ký tự rác (vd 'Web Attack � Brute Force') + map nhóm
# ---------------------------------------------------------------------------
df = df.withColumn("label", F.trim(F.regexp_replace(
    F.regexp_replace("label", r"[^\x00-\x7F]+", "-"), r"\s*-\s*", "-")))
# 'DoS Hulk'... không có dấu '-' nên không bị ảnh hưởng; 'FTP-Patator' giữ nguyên

mapping = F.create_map(*[F.lit(x) for kv in LABEL_TO_CATEGORY.items() for x in kv])
df = df.withColumn("attack_category", F.coalesce(mapping[F.col("label")], F.lit("unknown")))
df = df.withColumn("is_attack", (F.col("label") != "BENIGN").cast("int"))

# ---------------------------------------------------------------------------
# 5. Infinity / NaN -> null, sau đó loại dòng thiếu giá trị
# ---------------------------------------------------------------------------
for c in DOUBLE_COLUMNS:
    df = df.withColumn(c, F.when(F.isnan(c) | (F.abs(F.col(c)) == float("inf")), None)
                       .otherwise(F.col(c)))
df = df.dropna(subset=FEATURE_COLUMNS + ["label"])
log_step("drop_null_inf", df)

# ---------------------------------------------------------------------------
# 6. Loại giá trị âm vô lý (thời gian/tốc độ không thể âm).
#    Giữ init_win_bytes_* = -1 vì đó là giá trị sentinel của CICFlowMeter.
# ---------------------------------------------------------------------------
df = df.filter((F.col("flow_duration") >= 0)
               & (F.col("flow_bytes_s") >= 0)
               & (F.col("flow_packets_s") >= 0))
log_step("drop_negative", df)

# ---------------------------------------------------------------------------
# 7. Loại duplicate (chỉ xét feature + label, bỏ qua metadata nguồn)
# ---------------------------------------------------------------------------
df = df.dropDuplicates(FEATURE_COLUMNS + ["label"])
# Cột trùng hoàn toàn với fwd_header_length
df = df.drop("fwd_header_length_1").persist()
log_step("drop_duplicates", df)

# ---------------------------------------------------------------------------
# 8. Ghi Parquet, partition theo day
# ---------------------------------------------------------------------------
(df.repartition("day")
   .write.mode("overwrite")
   .partitionBy("day")
   .parquet(SPARK_CLEANED_DIR))
print(f"Đã ghi: {SPARK_CLEANED_DIR}")

# ---------------------------------------------------------------------------
# 9. Report
# ---------------------------------------------------------------------------
report["label_counts"] = {r["label"]: r["count"] for r in
                          df.groupBy("label").count().orderBy(F.desc("count")).collect()}
report["category_counts"] = {r["attack_category"]: r["count"] for r in
                             df.groupBy("attack_category").count().orderBy(F.desc("count")).collect()}
report["unknown_labels"] = [r["label"] for r in
                            df.filter(F.col("attack_category") == "unknown")
                            .select("label").distinct().collect()]
report["elapsed_seconds"] = round(time.time() - start, 1)

os.makedirs(REPORT_DIR, exist_ok=True)
with open(os.path.join(REPORT_DIR, "cleaning_report.json"), "w", encoding="utf-8") as f:
    json.dump(report, f, indent=2, ensure_ascii=False)

print(json.dumps(report, indent=2, ensure_ascii=False))
spark.stop()
