-- Tạo database
CREATE DATABASE IF NOT EXISTS cic_ids2017;
USE cic_ids2017;

-- 1.External Table 
CREATE EXTERNAL TABLE IF NOT EXISTS network_traffic_raw (
    destination_port INT,
    flow_duration BIGINT,
    total_fwd_packets INT,
    total_backward_packets INT,
    total_length_of_fwd_packets BIGINT,
    total_length_of_bwd_packets BIGINT,
    fwd_packet_length_max INT,
    fwd_packet_length_min INT,
    fwd_packet_length_mean DOUBLE,
    fwd_packet_length_std DOUBLE,
    bwd_packet_length_max INT,
    bwd_packet_length_min INT,
    bwd_packet_length_mean DOUBLE,
    bwd_packet_length_std DOUBLE,
    flow_bytes_s DOUBLE,
    flow_packets_s DOUBLE,
    flow_iat_mean DOUBLE,
    flow_iat_std DOUBLE,
    flow_iat_max BIGINT,
    flow_iat_min BIGINT,
    fwd_iat_total BIGINT,
    fwd_iat_mean DOUBLE,
    fwd_iat_std DOUBLE,
    fwd_iat_max BIGINT,
    fwd_iat_min BIGINT,
    bwd_iat_total BIGINT,
    bwd_iat_mean DOUBLE,
    bwd_iat_std DOUBLE,
    bwd_iat_max BIGINT,
    bwd_iat_min BIGINT,
    fwd_psh_flags INT,
    bwd_psh_flags INT,
    fwd_urg_flags INT,
    bwd_urg_flags INT,
    fwd_header_length INT,
    bwd_header_length INT,
    fwd_packets_s DOUBLE,
    bwd_packets_s DOUBLE,
    min_packet_length INT,
    max_packet_length INT,
    packet_length_mean DOUBLE,
    packet_length_std DOUBLE,
    packet_length_variance DOUBLE,
    fin_flag_count INT,
    syn_flag_count INT,
    rst_flag_count INT,
    psh_flag_count INT,
    ack_flag_count INT,
    urg_flag_count INT,
    cwe_flag_count INT,
    ece_flag_count INT,
    down_up_ratio DOUBLE,
    average_packet_size DOUBLE,
    avg_fwd_segment_size DOUBLE,
    avg_bwd_segment_size DOUBLE,
    fwd_header_length_1 INT,
    fwd_avg_bytes_bulk DOUBLE,
    fwd_avg_packets_bulk DOUBLE,
    fwd_avg_bulk_rate DOUBLE,
    bwd_avg_bytes_bulk DOUBLE,
    bwd_avg_packets_bulk DOUBLE,
    bwd_avg_bulk_rate DOUBLE,
    subflow_fwd_packets INT,
    subflow_fwd_bytes BIGINT,
    subflow_bwd_packets INT,
    subflow_bwd_bytes BIGINT,
    init_win_bytes_forward INT,
    init_win_bytes_backward INT,
    act_data_pkt_fwd INT,
    min_seg_size_forward INT,
    active_mean DOUBLE,
    active_std DOUBLE,
    active_max BIGINT,
    active_min BIGINT,
    idle_mean DOUBLE,
    idle_std DOUBLE,
    idle_max BIGINT,
    idle_min BIGINT,
    label STRING
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
LOCATION '/cic-ids2017/cleaned/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 2.Partition Table (Attack Type)
CREATE TABLE IF NOT EXISTS network_traffic_partitioned (
    destination_port INT,
    flow_duration BIGINT,
    total_fwd_packets INT,
    total_backward_packets INT,
    total_length_of_fwd_packets BIGINT,
    total_length_of_bwd_packets BIGINT,
    fwd_packet_length_max INT,
    fwd_packet_length_min INT,
    fwd_packet_length_mean DOUBLE,
    fwd_packet_length_std DOUBLE,
    bwd_packet_length_max INT,
    bwd_packet_length_min INT,
    bwd_packet_length_mean DOUBLE,
    bwd_packet_length_std DOUBLE,
    flow_bytes_s DOUBLE,
    flow_packets_s DOUBLE,
    flow_iat_mean DOUBLE,
    flow_iat_std DOUBLE,
    flow_iat_max BIGINT,
    flow_iat_min BIGINT,
    packet_length_mean DOUBLE,
    packet_length_std DOUBLE,
    packet_length_variance DOUBLE,
    average_packet_size DOUBLE
)
PARTITIONED BY (attack_type STRING)
STORED AS PARQUET;


SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.ppd=false;
SET hive.vectorized.execution.enabled=false;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- 3.Raw to Partitioned
INSERT OVERWRITE TABLE network_traffic_partitioned PARTITION(attack_type)
SELECT 
    destination_port, flow_duration, total_fwd_packets, total_backward_packets,
    total_length_of_fwd_packets, total_length_of_bwd_packets, fwd_packet_length_max,
    fwd_packet_length_min, fwd_packet_length_mean, fwd_packet_length_std,
    bwd_packet_length_max, bwd_packet_length_min, bwd_packet_length_mean,
    bwd_packet_length_std, flow_bytes_s, flow_packets_s, flow_iat_mean,
    flow_iat_std, flow_iat_max, flow_iat_min, packet_length_mean,
    packet_length_std, packet_length_variance, average_packet_size,
    label as attack_type
FROM network_traffic_raw;

