"""Kiểm tra Spark cluster: đọc HDFS + đọc bảng Hive qua metastore."""
from config import get_spark, CLEANED_DIR

spark = get_spark("smoke-test")
sc = spark.sparkContext
print(f"Spark {spark.version} | master = {sc.master}")

# 1. Job phân tán đơn giản
print("sum(0..999999) =", sc.parallelize(range(1_000_000), 8).sum())

# 2. Đọc CSV trực tiếp từ HDFS
df = spark.read.option("header", True).csv(CLEANED_DIR)
print(f"Rows in {CLEANED_DIR}: {df.count():,} | columns: {len(df.columns)}")

# 3. Đọc bảng Hive qua metastore
spark.sql("SHOW DATABASES").show()
spark.sql("""
    SELECT attack_type, COUNT(*) AS total_flows
    FROM cic_ids2017.network_traffic_partitioned
    GROUP BY attack_type
    ORDER BY total_flows DESC
""").show(truncate=False)

spark.stop()
