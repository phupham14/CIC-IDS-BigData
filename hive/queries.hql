USE cic_ids2017;

-- ============================================================================
-- NHÓM 1: PHÂN BỐ LƯU LƯỢNG & TỶ LỆ TẤN CÔNG (TRAFFIC & ATTACK DISTRIBUTION)
-- ============================================================================

-- Query 1.1: Thống kê số lượng luồng mạng và sắp xếp theo tần suất xuất hiện của từng loại traffic
SELECT 
    attack_type, 
    COUNT(*) AS total_flows
FROM 
    network_traffic_partitioned 
GROUP BY 
    attack_type 
ORDER BY 
    total_flows DESC;

-- Query 1.2: Phân tích nhị phân (Binary Classification): Tỷ lệ Benign vs Malicious (Tấn công)
SELECT 
    CASE 
        WHEN attack_type = 'BENIGN' THEN 'Normal Traffic'
        ELSE 'Attack Traffic'
    END AS traffic_class,
    COUNT(*) AS total_flows,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM 
    network_traffic_partitioned
GROUP BY 
    CASE 
        WHEN attack_type = 'BENIGN' THEN 'Normal Traffic'
        ELSE 'Attack Traffic'
    END;


-- ============================================================================
-- NHÓM 2: ĐẶC TRƯNG HÀNH VI & THỜI GIAN LUỒNG (TEMPORAL & THROUGHPUT BEHAVIOR)
-- ============================================================================

-- Query 2.1: Thống kê thời gian tồn tại luồng mạng (Flow Duration: Min, Max, Avg)
-- [LỌC DỮ LIỆU ÂM]: flow_duration >= 0 (loại bỏ các dòng bị âm như -13)
SELECT 
    attack_type, 
    COUNT(*) AS valid_flow_count,
    ROUND(AVG(flow_duration), 2) AS avg_flow_duration_us,
    MIN(flow_duration) AS min_flow_duration_us,
    MAX(flow_duration) AS max_flow_duration_us
FROM 
    network_traffic_partitioned 
WHERE 
    flow_duration >= 0
GROUP BY 
    attack_type
ORDER BY 
    avg_flow_duration_us DESC;

-- Query 2.2: Phân tích tốc độ truyền tải thông lượng (Throughput: Bytes/s và Packets/s)
-- Giúp phát hiện các cuộc tấn công dồn dập (Volumetric DDoS) so với tấn công âm thầm (Slowloris/Infiltration)
-- [LỌC DỮ LIỆU ÂM]: flow_bytes_s >= 0 AND flow_packets_s >= 0 AND flow_duration > 0
SELECT 
    attack_type,
    ROUND(AVG(flow_bytes_s), 2) AS avg_bytes_per_second,
    ROUND(MAX(flow_bytes_s), 2) AS max_bytes_per_second,
    ROUND(AVG(flow_packets_s), 2) AS avg_packets_per_second
FROM 
    network_traffic_partitioned
WHERE 
    flow_duration > 0 
    AND flow_bytes_s >= 0 
    AND flow_packets_s >= 0
GROUP BY 
    attack_type
ORDER BY 
    avg_bytes_per_second DESC;

-- Query 2.3: Phân tích độ trễ giữa các gói tin (Flow Inter-Arrival Time - IAT)
-- Dấu hiệu: Tấn công tự động (Botnet/DoS) thường có khoảng cách thời gian giữa các gói (IAT) cực nhỏ và đều
-- [LỌC DỮ LIỆU ÂM]: flow_iat_mean >= 0 AND flow_iat_min >= 0
SELECT 
    attack_type,
    ROUND(AVG(flow_iat_mean), 2) AS avg_iat_mean,
    ROUND(AVG(flow_iat_std), 2) AS avg_iat_std,
    MIN(flow_iat_min) AS global_min_iat,
    MAX(flow_iat_max) AS global_max_iat
FROM 
    network_traffic_partitioned
WHERE 
    flow_iat_mean >= 0 
    AND flow_iat_min >= 0
GROUP BY 
    attack_type
ORDER BY 
    avg_iat_mean ASC;


-- ============================================================================
-- NHÓM 3: PHÂN TÍCH CỔNG MỤC TIÊU & TÍNH ĐA DẠNG DỊCH VỤ (TARGET & PORT ANALYSIS)
-- ============================================================================

-- Query 3.1: Top 10 Destination Ports bị truy cập nhiều nhất toàn hệ thống
-- [LỌC DỮ LIỆU ÂM]: destination_port >= 0 AND flow_duration >= 0
SELECT 
    destination_port, 
    COUNT(*) AS total_connections,
    COUNT(DISTINCT attack_type) AS distinct_attack_types_targeted
FROM 
    network_traffic_partitioned 
WHERE 
    destination_port >= 0 
    AND flow_duration >= 0
GROUP BY 
    destination_port 
ORDER BY 
    total_connections DESC 
LIMIT 10;

-- Query 3.2: Cổng đích chủ yếu mà từng loại tấn công nhắm tới (Port Targeting Profile)
-- Ví dụ: SSH-Patator (Port 22), Web Attacks (Port 80/443/8080), DoS Hulk/GoldenEye
SELECT 
    attack_type,
    destination_port,
    COUNT(*) AS attack_count
FROM 
    network_traffic_partitioned
WHERE 
    attack_type != 'BENIGN' 
    AND destination_port >= 0
GROUP BY 
    attack_type, 
    destination_port
HAVING 
    COUNT(*) > 100
ORDER BY 
    attack_type, 
    attack_count DESC;

-- Query 3.3: Nhận diện hành vi Port Scanning (Quét cổng dịch vụ)
-- Dấu hiệu PortScan: Truy cập rất nhiều port khác nhau nhưng thời gian luồng và số lượng packet cực nhỏ
SELECT 
    attack_type,
    COUNT(DISTINCT destination_port) AS scanned_ports_count,
    ROUND(AVG(total_fwd_packets), 2) AS avg_fwd_packets,
    ROUND(AVG(flow_duration), 2) AS avg_flow_duration
FROM 
    network_traffic_partitioned
WHERE 
    flow_duration >= 0
GROUP BY 
    attack_type
ORDER BY 
    scanned_ports_count DESC;


-- ============================================================================
-- NHÓM 4: BẤT ĐỐI XỨNG LƯU LƯỢNG & KÍCH THƯỚC GÓI TIN (ASYMMETRY & PACKET SIZE)
-- ============================================================================

-- Query 4.1: Đo lường tính bất đối xứng giữa gói tin gửi đi (Forward) và gửi về (Backward)
-- Bình thường (BENIGN) luồng TCP/HTTP có chiều hỏi và đáp khá cân bằng. 
-- Các đòn tấn công như DoS/DDoS thường gửi ồ ạt gói đi nhưng gần như không nhận phản hồi.
-- [LỌC DỮ LIỆU ÂM]: total_fwd_packets >= 0 AND total_backward_packets >= 0
SELECT 
    attack_type,
    ROUND(AVG(total_fwd_packets), 2) AS avg_fwd_packets,
    ROUND(AVG(total_backward_packets), 2) AS avg_bwd_packets,
    ROUND(AVG(total_length_of_fwd_packets), 2) AS avg_fwd_bytes,
    ROUND(AVG(total_length_of_bwd_packets), 2) AS avg_bwd_bytes,
    ROUND(AVG(total_fwd_packets) / NULLIF(AVG(total_backward_packets), 0), 2) AS fwd_to_bwd_packet_ratio
FROM 
    network_traffic_partitioned
WHERE 
    total_fwd_packets >= 0 
    AND total_backward_packets >= 0
GROUP BY 
    attack_type
ORDER BY 
    fwd_to_bwd_packet_ratio DESC;

-- Query 4.2: Kích thước gói tin trung bình và độ biến động (Packet Length & Variance)
-- [LỌC DỮ LIỆU ÂM]: packet_length_mean >= 0 AND packet_length_variance >= 0
SELECT 
    attack_type,
    ROUND(AVG(packet_length_mean), 2) AS avg_packet_length,
    ROUND(AVG(packet_length_variance), 2) AS avg_packet_variance,
    ROUND(AVG(average_packet_size), 2) AS avg_overall_packet_size
FROM 
    network_traffic_partitioned
WHERE 
    packet_length_mean >= 0 
    AND packet_length_variance >= 0
GROUP BY 
    attack_type
ORDER BY 
    avg_packet_length DESC;


-- ============================================================================
-- NHÓM 5: PHÂN TÍCH NÂNG CAO TRÊN BẢNG RAW (TCP FLAGS & ACTIVE/IDLE BEHAVIOR)
-- ============================================================================

-- Query 5.1: Phân tích cờ TCP (SYN, ACK, FIN, RST, PSH, URG) theo từng loại tấn công
-- Dấu hiệu: SYN Flood sẽ có tỷ lệ syn_flag_count bất thường mà không có ACK.
SELECT 
    label AS attack_label,
    SUM(syn_flag_count) AS total_syn_flags,
    SUM(ack_flag_count) AS total_ack_flags,
    SUM(fin_flag_count) AS total_fin_flags,
    SUM(rst_flag_count) AS total_rst_flags,
    SUM(psh_flag_count) AS total_psh_flags
FROM 
    network_traffic_raw
WHERE 
    flow_duration >= 0
GROUP BY 
    label
ORDER BY 
    total_syn_flags DESC;

-- Query 5.2: Phân tích khoảng thời gian hoạt động (Active) và thời gian nghỉ (Idle)
-- Dấu hiệu: Tấn công Slowloris giữ kết nối mở lâu và có idle time dài để làm cạn kiệt tài nguyên Web Server
-- [LỌC DỮ LIỆU ÂM]: active_mean >= 0 AND idle_mean >= 0
SELECT 
    label AS attack_label,
    ROUND(AVG(active_mean), 2) AS avg_active_time_us,
    ROUND(AVG(idle_mean), 2) AS avg_idle_time_us,
    MAX(idle_max) AS max_idle_time_us
FROM 
    network_traffic_raw
WHERE 
    flow_duration >= 0 
    AND active_mean >= 0 
    AND idle_mean >= 0
GROUP BY 
    label
ORDER BY 
    avg_idle_time_us DESC;


-- ============================================================================
-- NHÓM 6: TRUY VẾT NGUỒN GỐC DỮ LIỆU (DATA LINEAGE & FORENSICS)
-- ============================================================================

-- Query 6.1: Truy vết file CSV gốc trên HDFS tạo ra từng loại nhãn tấn công (Sử dụng Virtual Column)
SELECT 
    label,
    INPUT__FILE__NAME AS source_hdfs_file,
    COUNT(*) AS records_in_file
FROM 
    network_traffic_raw
GROUP BY 
    label, 
    INPUT__FILE__NAME
ORDER BY 
    label, 
    records_in_file DESC;
