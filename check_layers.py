"""逐层对账：每个 (日期 × 来源)，检查 ODS = fact + 隔离、fact = DWS、DWS = ADS"""
import os
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("check-layers")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.driver.memory", "4g").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

B = os.path.expanduser("~/tlc-spark-pipeline/data/warehouse")
K = ["pickup_year", "pickup_month", "pickup_day", "source_type"]

def by_day(df, name, col=None):
    agg = F.count("*") if col is None else F.sum(col)
    return df.groupBy(*K).agg(agg.alias(name))

ods = None
for s in ["yellow", "green", "fhv_hv"]:
    c = by_day(spark.read.parquet(f"{B}/ods/{s}").withColumn("source_type", F.lit(s)), "ods")
    ods = c if ods is None else ods.unionByName(c)

fact = by_day(spark.read.parquet(f"{B}/dwd/fact_trips"), "fact")
rej  = by_day(spark.read.parquet(f"{B}/dq/dwd_cleaning"), "rejected")
dws  = by_day(spark.read.parquet(f"{B}/dws/trips_daily"), "dws", "trip_count")
ads  = by_day(spark.read.parquet(f"{B}/ads/kpi_daily"), "ads", "trip_count")

r = (ods.join(fact, K, "left").join(rej, K, "left")
        .join(dws, K, "left").join(ads, K, "left").fillna(0)
        .withColumn("ok",
            (F.col("ods") == F.col("fact") + F.col("rejected")) &
            (F.col("fact") == F.col("dws")) &
            (F.col("dws") == F.col("ads"))))

days_in_ads = ads.select("pickup_year", "pickup_month", "pickup_day").distinct().count()
bad = r.filter(~F.col("ok"))

print(f"\nADS 中的天数: {days_in_ads} / 91")
print(f"对不上的 (日期×来源) 组合: {bad.count()}")
bad.orderBy(*K).show(50)

print("各来源 Q1 总量：")
r.groupBy("source_type").agg(*[F.sum(c).alias(c) for c in
    ["ods", "fact", "rejected", "dws", "ads"]]).orderBy("source_type").show()

spark.stop()
