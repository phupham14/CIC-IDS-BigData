echo "Creating HDFS..."

hdfs dfs -mkdir -p /cic-ids2017

hdfs dfs -mkdir -p /cic-ids2017/raw
hdfs dfs -mkdir -p /cic-ids2017/cleaned
hdfs dfs -mkdir -p /cic-ids2017/processed

#Data raw
hdfs dfs -mkdir -p /cic-ids2017/raw/monday
hdfs dfs -mkdir -p /cic-ids2017/raw/tuesday
hdfs dfs -mkdir -p /cic-ids2017/raw/wednesday
hdfs dfs -mkdir -p /cic-ids2017/raw/thursday
hdfs dfs -mkdir -p /cic-ids2017/raw/friday

echo "Cấu trúc thư mục trên HDFS:"
hdfs dfs -ls -R /cic-ids2017

echo "HDFS Setup Completed"
