from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType

L, D = LongType(), DoubleType()

# Schema tường minh cho MachineLearningCSV (79 cột, đúng thứ tự trong file gốc).
# Tên cột đã chuẩn hóa giống data/data_cleaning.py và hive/create_tables.hql.
COLUMNS = [
    ("destination_port", L), ("flow_duration", L),
    ("total_fwd_packets", L), ("total_backward_packets", L),
    ("total_length_of_fwd_packets", D), ("total_length_of_bwd_packets", D),
    ("fwd_packet_length_max", D), ("fwd_packet_length_min", D),
    ("fwd_packet_length_mean", D), ("fwd_packet_length_std", D),
    ("bwd_packet_length_max", D), ("bwd_packet_length_min", D),
    ("bwd_packet_length_mean", D), ("bwd_packet_length_std", D),
    ("flow_bytes_s", D), ("flow_packets_s", D),
    ("flow_iat_mean", D), ("flow_iat_std", D), ("flow_iat_max", D), ("flow_iat_min", D),
    ("fwd_iat_total", D), ("fwd_iat_mean", D), ("fwd_iat_std", D), ("fwd_iat_max", D), ("fwd_iat_min", D),
    ("bwd_iat_total", D), ("bwd_iat_mean", D), ("bwd_iat_std", D), ("bwd_iat_max", D), ("bwd_iat_min", D),
    ("fwd_psh_flags", L), ("bwd_psh_flags", L), ("fwd_urg_flags", L), ("bwd_urg_flags", L),
    ("fwd_header_length", L), ("bwd_header_length", L),
    ("fwd_packets_s", D), ("bwd_packets_s", D),
    ("min_packet_length", D), ("max_packet_length", D),
    ("packet_length_mean", D), ("packet_length_std", D), ("packet_length_variance", D),
    ("fin_flag_count", L), ("syn_flag_count", L), ("rst_flag_count", L), ("psh_flag_count", L),
    ("ack_flag_count", L), ("urg_flag_count", L), ("cwe_flag_count", L), ("ece_flag_count", L),
    ("down_up_ratio", D), ("average_packet_size", D),
    ("avg_fwd_segment_size", D), ("avg_bwd_segment_size", D),
    ("fwd_header_length_1", L),
    ("fwd_avg_bytes_bulk", D), ("fwd_avg_packets_bulk", D), ("fwd_avg_bulk_rate", D),
    ("bwd_avg_bytes_bulk", D), ("bwd_avg_packets_bulk", D), ("bwd_avg_bulk_rate", D),
    ("subflow_fwd_packets", L), ("subflow_fwd_bytes", L),
    ("subflow_bwd_packets", L), ("subflow_bwd_bytes", L),
    ("init_win_bytes_forward", L), ("init_win_bytes_backward", L),
    ("act_data_pkt_fwd", L), ("min_seg_size_forward", L),
    ("active_mean", D), ("active_std", D), ("active_max", D), ("active_min", D),
    ("idle_mean", D), ("idle_std", D), ("idle_max", D), ("idle_min", D),
    ("label", StringType()),
]

RAW_SCHEMA = StructType([StructField(name, dtype, True) for name, dtype in COLUMNS])
FEATURE_COLUMNS = [name for name, _ in COLUMNS if name != "label"]
DOUBLE_COLUMNS = [name for name, dtype in COLUMNS if dtype == D]

# Nhãn gốc (sau khi sửa encoding) -> nhóm tấn công (mục 6.1 proposal)
LABEL_TO_CATEGORY = {
    "BENIGN": "normal",
    "DoS Hulk": "dos",
    "DoS GoldenEye": "dos",
    "DoS slowloris": "dos",
    "DoS Slowhttptest": "dos",
    "DDoS": "ddos",
    "PortScan": "portscan",
    "FTP-Patator": "brute_force",
    "SSH-Patator": "brute_force",
    "Bot": "botnet",
    "Web Attack-Brute Force": "web_attack",
    "Web Attack-XSS": "web_attack",
    "Web Attack-Sql Injection": "web_attack",
    "Infiltration": "infiltration",
    "Heartbleed": "heartbleed",
}


def normalize_header(name):
    return name.strip().lower().replace(" ", "_").replace("/", "_").replace("-", "_")
