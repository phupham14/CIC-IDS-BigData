"""
Gộp kết quả các benchmark -> 2 file cho visualization (chạy trên host hoặc container):

  results/benchmark_results.csv   tất cả các lần đo (kể cả warm-up run=0)
  results/benchmark_summary.csv   median / min / max theo từng cấu hình (bỏ warm-up)
                                  + speedup, efficiency cho scalability
"""
import glob
import os

import pandas as pd

from common import RESULTS_DIR, KEY

parts = [pd.read_csv(p) for p in sorted(glob.glob(os.path.join(RESULTS_DIR, "b[0-9]*_*.csv")))]   # b1_, b2b3_, b4_ (không lấy benchmark_*.csv)
if not parts:
    raise SystemExit("Chưa có kết quả nào trong results/")
df = pd.concat(parts, ignore_index=True)
df.to_csv(os.path.join(RESULTS_DIR, "benchmark_results.csv"), index=False)

keys = list(KEY)
text_keys = [k for k in keys if k != "data_pct"]
df[text_keys] = df[text_keys].fillna("")
df["data_pct"] = df["data_pct"].astype("Int64")     # benchmark SQL không có data_pct -> <NA>
measured = df[df["run"] >= 1]
ok = measured[measured["status"] == "ok"]

summary = (ok.groupby(keys, dropna=False)
             .agg(median_s=("seconds", "median"), min_s=("seconds", "min"), max_s=("seconds", "max"),
                  n_runs=("seconds", "size"), rows=("rows", "first"),
                  peak_mem_mb=("peak_mem_mb", "median"))
             .reset_index())
summary["status"] = "ok"

# Các cấu hình lỗi (oom/timeout/skipped) vẫn giữ lại để biểu đồ thể hiện được "pandas không chạy nổi"
failed = (measured[measured["status"] != "ok"].groupby(keys, dropna=False)["status"].first().reset_index())
failed = failed.merge(summary[keys], on=keys, how="left", indicator=True)
failed = failed[failed["_merge"] == "left_only"].drop(columns="_merge")
summary = pd.concat([summary, failed], ignore_index=True)

# Scalability: speedup = T(1 core) / T(n cores), efficiency = speedup / n
scal = summary["benchmark"] == "scalability"
if scal.any():
    summary.loc[scal, "cores"] = summary.loc[scal, "config"].str.extract(r"cores=(\d+)")[0].astype(float)
    base = (summary[scal & (summary["cores"] == 1) & summary["data_pct"].notna()]
            [["task", "data_pct", "median_s"]].rename(columns={"median_s": "base_s"}))
    summary = summary.merge(base, on=["task", "data_pct"], how="left")
    summary.loc[scal, "speedup"] = (summary["base_s"] / summary["median_s"]).round(2)
    summary.loc[scal, "efficiency"] = (summary["speedup"] / summary["cores"]).round(2)
    summary = summary.drop(columns="base_s")

summary = summary.sort_values(keys).round({"median_s": 3, "min_s": 3, "max_s": 3})
summary.to_csv(os.path.join(RESULTS_DIR, "benchmark_summary.csv"), index=False)
print(summary.to_string(index=False))
