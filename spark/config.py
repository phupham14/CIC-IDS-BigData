from pyspark.sql import SparkSession

HDFS_URI = "hdfs://namenode:9000"
HDFS_BASE = f"{HDFS_URI}/cic-ids2017"
RAW_DIR = f"{HDFS_BASE}/raw"
CLEANED_DIR = f"{HDFS_BASE}/cleaned"
PROCESSED_DIR = f"{HDFS_BASE}/processed"
SPARK_CLEANED_DIR = f"{PROCESSED_DIR}/cleaned_spark"
FLOWS_PARQUET_DIR = f"{PROCESSED_DIR}/flows"
FLOWS_CSV_DIR = f"{PROCESSED_DIR}/flows_csv"
DIM_PORT_SERVICE_DIR = f"{PROCESSED_DIR}/dim_port_service"
DIM_ATTACK_CATEGORY_DIR = f"{PROCESSED_DIR}/dim_attack_category"
ANALYTICS_DIR = f"{PROCESSED_DIR}/analytics"

HIVE_DB = "cic_ids2017"

# Đường dẫn local (repo được mount vào /opt/project trong container)
LOCAL_RAW_DIR = "/opt/project/data/raw_data"
REPORT_DIR = "/opt/project/spark/reports"


def get_spark(app_name="cic-ids2017"):
    # Cấu hình cluster/HDFS/Hive nằm trong conf/spark-defaults.conf
    return SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()
