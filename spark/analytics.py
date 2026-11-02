"""
Giai đoạn 4 - Big Data Analytics bằng Spark SQL.

Input : Hive tables cic_ids2017.flows, dim_attack_category (output của transform.py)
Output: hdfs:///cic-ids2017/processed/analytics/<query>/   (Parquet, 1 file)
        spark/reports/analytics/<query>.csv                (cho visualization)
        spark/reports/analytics_timings.json               (thời gian chạy từng query)

Chạy tất cả:        spark-submit analytics.py
Chạy một vài query: spark-submit analytics.py temporal_label_timeline network_port_diversity
"""
import json
import os
import sys
import time

from config import get_spark, ANALYTICS_DIR, HIVE_DB, REPORT_DIR

DAY_ORDER = ("CASE day WHEN 'monday' THEN 1 WHEN 'tuesday' THEN 2 WHEN 'wednesday' THEN 3 "
             "WHEN 'thursday' THEN 4 ELSE 5 END")

LABEL_PROFILE = """
SELECT label, attack_category,
       COUNT(*)                                        AS flows,
       ROUND(AVG(flow_duration_s), 4)                  AS avg_duration_s,
       ROUND(percentile_approx(flow_duration_s, 0.5), 4) AS median_duration_s,
       ROUND(AVG(total_packets), 2)                    AS avg_packets,
       ROUND(STDDEV(total_packets), 2)                 AS std_packets,
       ROUND(AVG(total_bytes), 1)                      AS avg_bytes,
       ROUND(AVG(bytes_per_packet), 2)                 AS avg_bytes_per_packet,
       ROUND(AVG(flow_bytes_s), 1)                     AS avg_flow_bytes_s,
       ROUND(AVG(flow_packets_s), 1)                   AS avg_flow_packets_s,
       ROUND(AVG(flow_iat_mean), 1)                    AS avg_iat_mean_us,
       ROUND(AVG(fwd_bwd_packet_ratio), 2)             AS avg_fwd_bwd_packet_ratio,
       ROUND(AVG(fwd_bytes_share), 3)                  AS avg_fwd_bytes_share,
       ROUND(AVG(has_response) * 100, 2)               AS response_rate_pct
FROM flows
GROUP BY label, attack_category
"""

QUERIES = {
    # ======================================================================
    # A. ATTACK DISTRIBUTION
    # ======================================================================
    # A1. Phân bố theo nhãn gốc
    "dist_label": """
        SELECT label, attack_category,
               COUNT(*) AS flows,
               ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 4) AS pct
        FROM flows
        GROUP BY label, attack_category
        ORDER BY flows DESC
    """,
    # A2. Phân bố theo nhóm tấn công + mức độ mất cân bằng (JOIN dimension)
    "dist_category": """
        SELECT f.attack_category, d.description, d.severity,
               COUNT(*) AS flows,
               ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 4) AS pct,
               ROUND(MAX(n.normal_flows) / COUNT(*), 1) AS normal_to_class_ratio
        FROM flows f
        JOIN dim_attack_category d ON f.attack_category = d.attack_category
        CROSS JOIN (SELECT COUNT(*) AS normal_flows FROM flows
                    WHERE attack_category = 'normal') n
        GROUP BY f.attack_category, d.description, d.severity
        ORDER BY flows DESC
    """,
    # A3. Normal vs Attack theo ngày
    "dist_day": f"""
        SELECT day,
               COUNT(*) AS total_flows,
               SUM(is_attack) AS attack_flows,
               ROUND(AVG(is_attack) * 100, 2) AS attack_pct,
               COUNT(DISTINCT CASE WHEN is_attack = 1 THEN label END) AS attack_types
        FROM flows
        GROUP BY day
        ORDER BY {DAY_ORDER}
    """,

    # ======================================================================
    # B. TEMPORAL ANALYSIS (thời gian giả lập từ seq_index - xem transform.py)
    # ======================================================================
    # B1. Mật độ tấn công theo time bucket (100 bucket / file)
    #     time_bucket được đánh riêng cho từng file -> luôn group theo source_file
    "temporal_bucket_overview": f"""
        SELECT day, session, source_file, time_bucket,
               MIN(pseudo_time) AS bucket_start,
               COUNT(*) AS total_flows,
               SUM(is_attack) AS attack_flows,
               ROUND(AVG(is_attack) * 100, 2) AS attack_pct
        FROM flows
        GROUP BY day, session, source_file, time_bucket
        ORDER BY {DAY_ORDER}, MIN(pseudo_minute)
    """,
    # B2. Số flow tấn công theo bucket và nhóm (dạng long, dễ pivot khi vẽ heatmap)
    "temporal_bucket_category": f"""
        SELECT day, session, source_file, time_bucket,
               MIN(pseudo_time) AS bucket_start, attack_category, COUNT(*) AS flows
        FROM flows
        WHERE is_attack = 1
        GROUP BY day, session, source_file, time_bucket, attack_category
        ORDER BY {DAY_ORDER}, MIN(pseudo_minute), attack_category
    """,
    # B3. Timeline từng loại tấn công: dùng percentile thay vì min/max
    #     (flow dài kết thúc muộn làm max bị kéo về cuối file)
    "temporal_label_timeline": f"""
        SELECT day, session, source_file, label, flows,
               p05_bucket, median_bucket, p95_bucket,
               format_string('%02d:%02d', CAST(p05_min / 60 AS INT), p05_min % 60)       AS p05_time,
               format_string('%02d:%02d', CAST(median_min / 60 AS INT), median_min % 60) AS median_time,
               format_string('%02d:%02d', CAST(p95_min / 60 AS INT), p95_min % 60)       AS p95_time
        FROM (
            SELECT day, session, source_file, label, COUNT(*) AS flows,
                   percentile_approx(time_bucket, 0.05)   AS p05_bucket,
                   percentile_approx(time_bucket, 0.5)    AS median_bucket,
                   percentile_approx(time_bucket, 0.95)   AS p95_bucket,
                   percentile_approx(pseudo_minute, 0.05) AS p05_min,
                   percentile_approx(pseudo_minute, 0.5)  AS median_min,
                   percentile_approx(pseudo_minute, 0.95) AS p95_min
            FROM flows
            WHERE is_attack = 1
            GROUP BY day, session, source_file, label
        ) t
        ORDER BY {DAY_ORDER}, median_min
    """,
    # B4. Theo giờ giả lập
    "temporal_hourly": f"""
        SELECT day, pseudo_hour,
               COUNT(*) AS total_flows,
               SUM(is_attack) AS attack_flows,
               ROUND(AVG(is_attack) * 100, 2) AS attack_pct
        FROM flows
        GROUP BY day, pseudo_hour
        ORDER BY {DAY_ORDER}, pseudo_hour
    """,

    # ======================================================================
    # C. NETWORK ANALYSIS (destination_port / service)
    # ======================================================================
    # C1. Top 20 destination port toàn hệ thống
    "network_top_ports": """
        SELECT destination_port, service, service_group,
               COUNT(*) AS flows,
               SUM(is_attack) AS attack_flows,
               ROUND(AVG(is_attack) * 100, 2) AS attack_pct,
               COUNT(DISTINCT CASE WHEN is_attack = 1 THEN label END) AS attack_types
        FROM flows
        GROUP BY destination_port, service, service_group
        ORDER BY flows DESC
        LIMIT 20
    """,
    # C2. Top 5 port mà mỗi loại tấn công nhắm tới
    "network_ports_per_attack": """
        SELECT label, rnk, destination_port, service, flows, pct_of_label
        FROM (
            SELECT label, destination_port, service, flows,
                   ROW_NUMBER() OVER (PARTITION BY label ORDER BY flows DESC) AS rnk,
                   ROUND(flows * 100.0 / SUM(flows) OVER (PARTITION BY label), 2) AS pct_of_label
            FROM (SELECT label, destination_port, service, COUNT(*) AS flows
                  FROM flows WHERE is_attack = 1
                  GROUP BY label, destination_port, service) pc
        ) r
        WHERE rnk <= 5
        ORDER BY label, rnk
    """,
    # C3. Phân bố dải port (well-known / registered / dynamic) theo nhóm
    "network_port_range": """
        SELECT attack_category, port_range,
               COUNT(*) AS flows,
               ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY attack_category), 2) AS pct
        FROM flows
        GROUP BY attack_category, port_range
        ORDER BY attack_category, port_range
    """,
    # C4. Độ đa dạng port: số port khác nhau, entropy, tỷ trọng port phổ biến nhất
    #     -> PortScan có entropy cao, DoS/Brute force tập trung vào 1 port
    "network_port_diversity": """
        SELECT pc.label,
               MAX(t.total) AS flows,
               MAX(t.n_ports) AS distinct_ports,
               ROUND(-SUM((pc.c / t.total) * LOG2(pc.c / t.total)), 3) AS port_entropy_bits,
               ROUND(MAX(pc.c) / MAX(t.total), 4) AS top_port_share
        FROM (SELECT label, destination_port, COUNT(*) AS c
              FROM flows GROUP BY label, destination_port) pc
        JOIN (SELECT label, COUNT(*) AS total, COUNT(DISTINCT destination_port) AS n_ports
              FROM flows GROUP BY label) t
          ON pc.label = t.label
        GROUP BY pc.label
        ORDER BY port_entropy_bits DESC
    """,
    # C5. Nhóm dịch vụ bị nhắm tới theo nhóm tấn công
    "network_service_group": """
        SELECT attack_category, service_group,
               COUNT(*) AS flows,
               ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY attack_category), 2) AS pct
        FROM flows
        GROUP BY attack_category, service_group
        ORDER BY attack_category, flows DESC
    """,

    # ======================================================================
    # D. ATTACK COMPARISON (normal vs từng loại tấn công)
    # ======================================================================
    # D1. Hồ sơ thống kê của từng nhãn
    "compare_label_profile": LABEL_PROFILE + " ORDER BY flows DESC",
    # D2. Bội số so với BENIGN (x lần lưu lượng bình thường)
    "compare_vs_benign": f"""
        WITH p AS ({LABEL_PROFILE}),
             b AS (SELECT * FROM p WHERE label = 'BENIGN')
        SELECT p.label, p.attack_category, p.flows,
               ROUND(p.median_duration_s / NULLIF(b.median_duration_s, 0), 2) AS median_duration_x,
               ROUND(p.avg_packets / b.avg_packets, 2)                     AS packets_x,
               ROUND(p.avg_bytes / b.avg_bytes, 2)                         AS bytes_x,
               ROUND(p.avg_bytes_per_packet / b.avg_bytes_per_packet, 2)   AS bytes_per_packet_x,
               ROUND(p.avg_flow_packets_s / b.avg_flow_packets_s, 2)       AS packet_rate_x,
               ROUND(p.avg_iat_mean_us / b.avg_iat_mean_us, 2)             AS iat_x,
               ROUND(p.response_rate_pct - b.response_rate_pct, 2)         AS response_rate_diff_pp
        FROM p CROSS JOIN b
        WHERE p.label <> 'BENIGN'
        ORDER BY p.flows DESC
    """,
    # D3. Cờ TCP trung bình mỗi flow theo nhãn
    "compare_tcp_flags": """
        SELECT label, attack_category, COUNT(*) AS flows,
               ROUND(AVG(syn_flag_count), 4) AS avg_syn,
               ROUND(AVG(ack_flag_count), 4) AS avg_ack,
               ROUND(AVG(fin_flag_count), 4) AS avg_fin,
               ROUND(AVG(rst_flag_count), 4) AS avg_rst,
               ROUND(AVG(psh_flag_count), 4) AS avg_psh,
               ROUND(AVG(urg_flag_count), 4) AS avg_urg,
               ROUND(AVG(init_win_bytes_forward), 1) AS avg_init_win_fwd
        FROM flows
        GROUP BY label, attack_category
        ORDER BY flows DESC
    """,
}


def main(selected):
    unknown = [q for q in selected if q not in QUERIES]
    if unknown:
        sys.exit(f"Query không tồn tại: {unknown}\nCó: {list(QUERIES)}")

    spark = get_spark("analytics")
    spark.sql(f"USE {HIVE_DB}")

    local_dir = os.path.join(REPORT_DIR, "analytics")
    os.makedirs(local_dir, exist_ok=True)
    timings_path = os.path.join(REPORT_DIR, "analytics_timings.json")
    timings = {}
    if os.path.exists(timings_path):
        with open(timings_path, encoding="utf-8") as f:
            timings = json.load(f)

    for name in selected or QUERIES:
        print(f"\n===== {name} =====")
        t = time.time()
        result = spark.sql(QUERIES[name]).coalesce(1).persist()
        result.write.mode("overwrite").parquet(f"{ANALYTICS_DIR}/{name}")
        elapsed = round(time.time() - t, 2)

        pdf = result.toPandas()
        pdf.to_csv(os.path.join(local_dir, f"{name}.csv"), index=False, encoding="utf-8")
        result.unpersist()

        timings[name] = {"seconds": elapsed, "rows": len(pdf)}
        print(pdf.head(15).to_string(index=False))
        print(f"-> {len(pdf)} rows, {elapsed}s")

    with open(timings_path, "w", encoding="utf-8") as f:
        json.dump(timings, f, indent=2)
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
