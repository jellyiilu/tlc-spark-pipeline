"""
分区裁剪效果测试：对比按日分区 vs 不分区两种存储下，单日查询的扫描量与耗时。

前提：已运行 01_ods_ingest.ipynb，在 data/warehouse/dws/trips_daily 中有多日数据。
运行：conda activate tlc-spark && python benchmarks/benchmark_partition_pruning.py
"""
import glob
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("TLC-Partition-Benchmark")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

BASE         = os.path.expanduser("~/tlc-spark-pipeline/data/warehouse/dws")
PART         = f"{BASE}/trips_daily"           # 按 pickup_year / pickup_month / pickup_day 分区
FLAT         = f"{BASE}/trips_daily_flat"      # 不分区的对照版本（自动生成）
TARGET_YEAR  = 2024
TARGET_MONTH = 3
TARGET_DAY   = 15


def parquet_files(path):
    return glob.glob(f"{path}/**/*.parquet", recursive=True)


def dir_bytes(path):
    return sum(os.path.getsize(f) for f in parquet_files(path))


def bench(df, runs=3):
    """先预热一次，再跑 runs 次取中位数，避免首次 JVM / 缓存影响"""
    q = df.agg(F.sum("gmv"), F.avg("trip_count"))
    q.collect()
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        q.collect()
        times.append(time.perf_counter() - t0)
    return sorted(times)[runs // 2]


# 1. 生成不分区的对照数据（只需生成一次）
if not os.path.exists(FLAT):
    spark.read.parquet(PART).write.mode("overwrite").parquet(FLAT)
    print(f"Generated flat baseline at {FLAT}")

part_df = (
    spark.read.parquet(PART)
    .filter((F.col("pickup_year") == TARGET_YEAR)
            & (F.col("pickup_month") == TARGET_MONTH)
            & (F.col("pickup_day") == TARGET_DAY))
)
flat_df = (
    spark.read.parquet(FLAT)
    .filter((F.col("pickup_year") == TARGET_YEAR)
            & (F.col("pickup_month") == TARGET_MONTH)
            & (F.col("pickup_day") == TARGET_DAY))
)

# 2. 确认分区裁剪生效
print("=== 分区版本执行计划 ===")
part_df.explain()

# 3. 扫描量对比
total_bytes = dir_bytes(PART)
day_path    = (f"{PART}/pickup_year={TARGET_YEAR}"
               f"/pickup_month={TARGET_MONTH}"
               f"/pickup_day={TARGET_DAY}")
day_bytes = dir_bytes(day_path) if os.path.exists(day_path) else 0
print("\n=== 扫描量 ===")
print(f"全量数据: {total_bytes / 1e6:.1f} MB, {len(parquet_files(PART))} 个文件")
print(f"{TARGET_YEAR}-{TARGET_MONTH:02d}-{TARGET_DAY:02d} 分区: "
      f"{day_bytes / 1e6:.1f} MB, {len(parquet_files(day_path)) if os.path.exists(day_path) else 0} 个文件")
if total_bytes > 0:
    print(f"扫描量降低: {(1 - day_bytes / total_bytes) * 100:.1f}%")

# 4. 查询耗时对比
t_part = bench(part_df)
t_flat = bench(flat_df)
print("\n=== 单日查询耗时（中位数）===")
print(f"分区版本:   {t_part:.2f}s")
print(f"不分区版本: {t_flat:.2f}s")
if t_flat > 0:
    print(f"耗时降低:   {(1 - t_part / t_flat) * 100:.1f}%")

print("\n=== 运行环境 ===")
print(f"Spark {spark.version}, CPU 核数 {os.cpu_count()}, driver 内存 4g, local 模式")

spark.stop()
