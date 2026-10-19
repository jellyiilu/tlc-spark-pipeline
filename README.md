# TLC Trip Data Pipeline — PySpark + Kafka + Airflow

A local batch and streaming data pipeline built on NYC TLC Yellow + Green Taxi trip records (Q1 2024, ~9.7M raw rows).

## Architecture

```
data/raw/yellow/*.parquet  data/raw/green/*.parquet
         (NYC TLC source data — two taxi types)
                    ↓
01_ods_ingest.ipynb         ODS layer: ingest raw → add source_type + ingestion_ts metadata
                    ↓
data/warehouse/ods/         Partitioned by pickup_year / pickup_month
                    ↓
02_dwd_build.ipynb          DWD layer: unify schemas → clean → derive columns → star schema
                    ↓
data/warehouse/dwd/         fact_trips / dim_zone / dim_date
                    ↓
03_dws_build.ipynb          DWS layer: daily zone-level aggregation (salt-based skew handling)
                    ↓
data/warehouse/dws/         trips_daily (partitioned by year/month/day)
                    ↓
04_ads_metrics.ipynb        ADS layer: KPI metrics (trip count, GMV, surcharge rate, tip rate)
                    ↓
data/warehouse/ads/         kpi_daily
                    ↓
05_data_quality.ipynb       13 DQ rules → quarantine table (blocking on FAIL)
                    ↓
dags/tlc_pipeline.py        Airflow DAG orchestrating all 6 tasks with retry / backfill
```

```
data/warehouse/dwd/fact_trips/
         ↓
02_kafka_producer.ipynb     Kafka producer: replay yellow records as real-time stream
         ↓
Kafka broker (tlc-trips)    3-partition topic on localhost:9092
         ↓
03_spark_streaming.ipynb    Spark Structured Streaming: 5-min event-time window aggregations
                            by pickup zone → foreachBatch write to Parquet + console
         ↓
Reconciliation cell         Compare streaming trip_count vs offline DWD for same window
```

## Tech Stack

- **PySpark 3.5.1** — batch ETL (ODS/DWD/DWS/ADS) and structured streaming
- **Kafka 3.7.1** — message broker (KRaft mode, no Zookeeper)
- **Airflow 2.9.3** — DAG orchestration, retry, backfill
- **kafka-python** — producer client
- **pandas + pyarrow** — parquet I/O for producer

## Notebooks

| Notebook | Layer | Description |
|---|---|---|
| `01_ods_ingest.ipynb` | ODS | Ingest Yellow + Green + FHV HV raw parquet → add source_type / ingestion_ts / partition columns → write partitioned ODS |
| `02_dq_after_ods.ipynb` | DQ Gate 1 | Row count non-empty per source; daily count vs 7-day avg ≤30% — FAIL blocks DWD |
| `03_dwd_build.ipynb` | DWD | Unify schemas (rename FHV HV fields, add null passenger_count) → clean → derive columns → write fact_trips + dim_zone + dim_date; dynamic partition overwrite |
| `04_dq_after_dwd.ipynb` | DQ Gate 2 | Rules 1–12: completeness / range / temporal / row conservation / referential integrity; violations to quarantine table — FAIL blocks DWS |
| `05_dws_build.ipynb` | DWS | Daily zone-level aggregation; salt-based two-phase aggregation for hotspot skew |
| `06_dq_after_dws.ipynb` | DQ Gate 3 | DWS trip_count == fact_trips count within 0.1% tolerance — FAIL blocks ADS |
| `07_ads_metrics.ipynb` | ADS | KPI: trip_count, GMV, surcharge_rate, tip_rate, avg_fare, avg_distance, avg_duration |
| `08_kafka_producer.ipynb` | Stream | Replay yellow fact_trips records to Kafka at 20ms intervals, keyed by pickup zone |
| `09_spark_streaming.ipynb` | Stream | Subscribe to tlc-trips → 5-min event-time tumbling window by PULocationID → foreachBatch write Parquet + reconcile vs offline DWD |

## Airflow DAG

`dags/tlc_pipeline.py` — 9 tasks, daily schedule (`0 6 * * *`), retry=2:

```
start → ods_ingest_yellow ─┐
      → ods_ingest_green  ─┼─ dq_after_ods → dwd_build → dq_after_dwd → dws_build → dq_after_dws → ads_metrics → end
      → ods_ingest_fhv    ─┘
```

## Setup

### Prerequisites
- Python 3.10 (conda env)
- Java 11+
- Kafka 3.7.1 (download from kafka.apache.org, extract to ~/kafka/)

### Install dependencies
```bash
conda create -n tlc-spark python=3.10 -y
conda activate tlc-spark
pip install pyspark==3.5.1 kafka-python pandas pyarrow jupyter ipykernel \
  apache-airflow==2.9.3 \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.10.txt"
python -m ipykernel install --user --name tlc-spark --display-name "tlc-spark"
```

### Download TLC data
```bash
mkdir -p data/raw/yellow data/raw/green
# Yellow
curl -o data/raw/yellow/yellow_tripdata_2024-01.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
curl -o data/raw/yellow/yellow_tripdata_2024-02.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet
curl -o data/raw/yellow/yellow_tripdata_2024-03.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet
# Green
curl -o data/raw/green/green_tripdata_2024-01.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet
curl -o data/raw/green/green_tripdata_2024-02.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-02.parquet
curl -o data/raw/green/green_tripdata_2024-03.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-03.parquet
```

### Start Kafka
```bash
~/kafka/bin/kafka-server-start.sh ~/kafka/config/kraft/server.properties

# Create topic (new terminal)
~/kafka/bin/kafka-topics.sh --create \
  --topic tlc-trips --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1
```

### Run batch pipeline
```bash
# Run notebooks 01-05 in order (kernel: tlc-spark)
jupyter nbconvert --to notebook --execute --inplace \
  --ExecutePreprocessor.kernel_name=tlc-spark \
  notebooks/01_ods_ingest.ipynb notebooks/02_dwd_build.ipynb \
  notebooks/03_dws_build.ipynb  notebooks/04_ads_metrics.ipynb \
  notebooks/05_data_quality.ipynb
```

### Run streaming
```bash
# 1. Run Kafka producer
jupyter nbconvert --to notebook --execute --inplace \
  --ExecutePreprocessor.kernel_name=tlc-spark notebooks/02_kafka_producer.ipynb
# 2. Run streaming (reads from startingOffsets=earliest)
jupyter nbconvert --to notebook --execute --inplace \
  --ExecutePreprocessor.kernel_name=tlc-spark notebooks/03_spark_streaming.ipynb
```

## Data Quality Results (Q1 2024)

| Rule | Layer | Type | Result |
|---|---|---|---|
| fare_amount not null | ODS | completeness | ✅ PASS |
| pickup_datetime not null | ODS | completeness | ✅ PASS |
| PULocationID not null | ODS | completeness | ✅ PASS |
| DOLocationID not null | ODS | completeness | ✅ PASS |
| passenger_count not null | ODS | completeness | ❌ FAIL — 760,402 nulls (7.8%) |
| fare_amount > 0 | ODS | range | ✅ PASS |
| trip_distance >= 0 | ODS | range | ✅ PASS |
| passenger_count in [1,6] | ODS | range | ✅ PASS |
| dropoff > pickup | ODS | temporal | ✅ PASS |
| pickup in 2024-Q1 | ODS | temporal | ✅ PASS |
| fact >= ODS × 0.80 | DWD | row conservation | ✅ PASS |
| PULocationID in dim_zone | DWD | referential | ✅ PASS |
| DWS trips ≈ fact rows | DWS | agg conservation | ✅ PASS |

**Quarantine rate: 10.17%** (988,678 / 9,722,363 rows isolated)

## Key Metrics (Q1 2024)

| Source | Trips | GMV | Surcharge Rate | Avg Fare |
|---|---|---|---|---|
| Yellow | 8,447,443 | ~$234M | ~21% | ~$18.6 |
| Green | 146,590 | ~$3.2M | ~15% | ~$16.3 |
