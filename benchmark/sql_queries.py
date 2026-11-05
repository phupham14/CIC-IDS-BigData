"""
B2 (CSV vs Parquet) + B3 (Hive vs Spark SQL) - bộ truy vấn chung.

Cú pháp viết sao cho chạy được trên CẢ Hive 2.3 và Spark SQL 3.5.
{t} được thay bằng cic_ids2017.flows (Parquet) hoặc cic_ids2017.flows_csv (CSV).
"""
TABLES = ["flows", "flows_csv"]

QUERIES = {
    # Đếm toàn bảng: Parquet có thể trả lời từ metadata (row count trong footer)
    "count": "SELECT COUNT(*) FROM {t}",

    # GROUP BY đọc ít cột -> lợi thế column pruning của Parquet
    "groupby_agg": """
        SELECT label, COUNT(*), AVG(flow_duration), AVG(total_bytes)
        FROM {t} GROUP BY label""",

    # Lọc trên cột thường
    "filter": """
        SELECT COUNT(*), AVG(bytes_per_packet)
        FROM {t} WHERE destination_port = 80 AND flow_duration > 1000000""",

    # Lọc trên cột partition -> partition pruning (chỉ đọc thư mục attack_category=dos)
    "partition_filter": """
        SELECT label, COUNT(*), AVG(flow_bytes_s)
        FROM {t} WHERE attack_category = 'dos' GROUP BY label""",

    # JOIN với bảng dimension
    "join": """
        SELECT d.severity, COUNT(*), AVG(f.flow_bytes_s)
        FROM {t} f JOIN cic_ids2017.dim_attack_category d ON f.attack_category = d.attack_category
        GROUP BY d.severity""",

    # Window function: top 3 port mỗi nhãn
    "window": """
        SELECT label, destination_port, cnt FROM (
            SELECT label, destination_port, cnt,
                   ROW_NUMBER() OVER (PARTITION BY label ORDER BY cnt DESC) AS rn
            FROM (SELECT label, destination_port, COUNT(*) AS cnt
                  FROM {t} GROUP BY label, destination_port) g
        ) r WHERE rn <= 3""",

    # Aggregate trên nhiều cột -> đọc nhiều cột hơn (giảm lợi thế columnar)
    "wide_agg": """
        SELECT attack_category,
               AVG(flow_iat_mean), AVG(fwd_iat_mean), AVG(bwd_iat_mean),
               AVG(active_mean), AVG(idle_mean), AVG(packet_length_mean),
               AVG(packet_length_std), AVG(init_win_bytes_forward), AVG(init_win_bytes_backward),
               SUM(syn_flag_count), SUM(ack_flag_count), SUM(fin_flag_count), SUM(psh_flag_count)
        FROM {t} GROUP BY attack_category""",
}


def render(query, table):
    return " ".join(QUERIES[query].format(t=f"cic_ids2017.{table}").split())
