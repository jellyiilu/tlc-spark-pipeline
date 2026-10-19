"""
分区裁剪效果测试：对比"按年月分区"与"不分区"两种存储方式下，单月查询的扫描量与耗时。
前提：已运行 01_batch_etl.ipynb，生成 data/processed/trips/ 分区数据。
运行：conda activate tlc-spark && python benchmark_partition_pruning.py
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

BASE = os.path.expanduser("~/tlc-spark-pipeline/data/processed")
PART = f"{BASE}/trips"          # 按 pickup_year / pickup_month 分区
FLAT = f"{BASE}/trips_flat"     # 不分区的对照版本
TARGET_MONTH = 3


def parquet_files(path):
    return glob.glob(f"{path}/**/*.parquet", recursive=True)


def dir_bytes(path):
    return sum(os.path.getsize(f) for f in parquet_files(path))


def bench(df, runs=3):
    """先预热一次，再跑 runs 次取中位数，避免首次 JVM / 缓存影响"""
    q = df.agg(F.sum("fare_amount"), F.avg("trip_duration_min"))
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

part_df = spark.read.parquet(PART).filter(F.col("pickup_month") == TARGET_MONTH)
flat_df = spark.read.parquet(FLAT).filter(F.col("pickup_month") == TARGET_MONTH)

# 2. 确认分区裁剪生效：输出中应出现 PartitionFilters: [..., (pickup_month = 3)]
print("=== 分区版本执行计划 ===")
part_df.explain()

# 3. 扫描量对比（分区版本只需读取目标月份目录）
total_bytes = dir_bytes(PART)
month_path = f"{PART}/pickup_year=2024/pickup_month={TARGET_MONTH}"
month_bytes = dir_bytes(month_path)
print("\n=== 扫描量 ===")
print(f"全量数据: {total_bytes / 1e6:.1f} MB, {len(parquet_files(PART))} 个文件")
print(f"{TARGET_MONTH} 月分区: {month_bytes / 1e6:.1f} MB, {len(parquet_files(month_path))} 个文件")
print(f"扫描量降低: {(1 - month_bytes / total_bytes) * 100:.1f}%")

# 4. 查询耗时对比
t_part = bench(part_df)
t_flat = bench(flat_df)
print("\n=== 单月查询耗时（中位数）===")
print(f"分区版本: {t_part:.2f}s")
print(f"不分区版本: {t_flat:.2f}s")
print(f"耗时降低: {(1 - t_part / t_flat) * 100:.1f}%")

# 5. 记录运行环境，面试时一起说明
print("\n=== 运行环境 ===")
print(f"Spark {spark.version}, CPU 核数 {os.cpu_count()}, driver 内存 4g, local 模式")

spark.stop()
