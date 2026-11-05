"""B2 + B3 (phần Spark SQL) - spark-submit bench_sql_spark.py [--queries ...] [--tables ...]"""
import argparse
import time

from common import now, row, save, parse_list
from config import get_spark
from sql_queries import QUERIES, TABLES, render

parser = argparse.ArgumentParser()
parser.add_argument("--queries", type=parse_list, default=list(QUERIES))
parser.add_argument("--tables", type=parse_list, default=TABLES)
parser.add_argument("--runs", type=int, default=3)
args = parser.parse_args()

spark = get_spark("benchmark-sql-spark")
spark.sparkContext.setLogLevel("ERROR")
rows = []

for table in args.tables:
    for query in args.queries:
        sql = render(query, table)
        for run in range(0, args.runs + 1):          # run 0 = warm-up
            started = now()
            t = time.perf_counter()
            try:
                n, status = len(spark.sql(sql).collect()), "ok"
            except Exception as e:
                n, status = None, f"error: {str(e).splitlines()[0][:100]}"
            seconds = round(time.perf_counter() - t, 3)
            rows.append(row(benchmark="sql_engine", engine="spark_sql", task=query, table=table,
                            data_pct=100, config="default", run=run, seconds=seconds,
                            rows=n, status=status, started_at=started))
            print(f"spark_sql {table:<9} {query:<16} run {run}: {seconds}s rows={n} {status}")

save("b2b3_sql_spark", rows)
spark.stop()
