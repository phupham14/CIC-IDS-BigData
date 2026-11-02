"""Bảng dimension nhỏ dùng để join với bảng fact network flows."""
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# destination_port -> service (các port phổ biến trong CIC-IDS2017)
PORT_SERVICES = [
    (0, "port-0", "other"), (20, "ftp-data", "file_transfer"), (21, "ftp", "file_transfer"),
    (22, "ssh", "remote_access"), (23, "telnet", "remote_access"), (25, "smtp", "email"),
    (53, "dns", "infrastructure"), (67, "dhcp", "infrastructure"), (68, "dhcp", "infrastructure"),
    (80, "http", "web"), (88, "kerberos", "auth"), (110, "pop3", "email"),
    (123, "ntp", "infrastructure"), (135, "msrpc", "windows"), (137, "netbios-ns", "windows"),
    (138, "netbios-dgm", "windows"), (139, "netbios-ssn", "windows"), (143, "imap", "email"),
    (161, "snmp", "infrastructure"), (389, "ldap", "auth"), (443, "https", "web"),
    (444, "snpp/heartbleed-lab", "web"), (445, "smb", "windows"), (465, "smtps", "email"),
    (993, "imaps", "email"), (995, "pop3s", "email"), (1900, "ssdp", "infrastructure"),
    (3268, "ldap-gc", "auth"), (3389, "rdp", "remote_access"), (5353, "mdns", "infrastructure"),
    (8080, "http-alt", "web"), (8443, "https-alt", "web"),
]
PORT_SERVICE_SCHEMA = StructType([
    StructField("destination_port", IntegerType(), False),
    StructField("service", StringType(), False),
    StructField("service_group", StringType(), False),
])

# attack_category -> mô tả + mức độ nghiêm trọng (1 thấp - 5 cao, do nhóm tự quy ước)
ATTACK_CATEGORIES = [
    ("normal", "Lưu lượng bình thường", 0),
    ("dos", "Denial of Service từ một nguồn (Hulk, GoldenEye, slowloris, Slowhttptest)", 4),
    ("ddos", "Distributed Denial of Service (LOIC)", 5),
    ("portscan", "Quét cổng dịch vụ (nmap)", 2),
    ("brute_force", "Dò mật khẩu FTP/SSH (Patator)", 3),
    ("web_attack", "Tấn công ứng dụng web (Brute Force, XSS, SQL Injection)", 4),
    ("botnet", "Máy bị nhiễm botnet (Ares)", 4),
    ("infiltration", "Xâm nhập mạng nội bộ", 5),
    ("heartbleed", "Khai thác lỗ hổng OpenSSL Heartbleed", 5),
]
ATTACK_CATEGORY_SCHEMA = StructType([
    StructField("attack_category", StringType(), False),
    StructField("description", StringType(), False),
    StructField("severity", IntegerType(), False),
])

# Khung giờ thu thập (xấp xỉ) của từng session để sinh pseudo time từ seq_index.
# Đây là thời gian GIẢ LẬP: chỉ phản ánh thứ tự tương đối, không phải timestamp thật.
SESSION_WINDOWS = {          # (giờ bắt đầu, giờ kết thúc) tính theo phút trong ngày
    "fullday": (9 * 60, 17 * 60),
    "morning": (9 * 60, 12 * 60),
    "afternoon": (13 * 60, 17 * 60),
}
# Thứ 6 buổi chiều có 2 file thu riêng -> tách khung giờ theo thứ tự trong lịch tấn công
# CIC công bố (PortScan 13:55-14:35, DDoS 15:56-16:16), tránh 2 file chồng lên nhau.
FILE_WINDOWS = {
    "Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX": (13 * 60, 15 * 60),
    "Friday-WorkingHours-Afternoon-DDos.pcap_ISCX": (15 * 60, 17 * 60),
}


def port_service_df(spark):
    return spark.createDataFrame(PORT_SERVICES, PORT_SERVICE_SCHEMA)


def attack_category_df(spark):
    return spark.createDataFrame(ATTACK_CATEGORIES, ATTACK_CATEGORY_SCHEMA)
