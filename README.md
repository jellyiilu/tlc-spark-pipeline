# TLC Trip Data Pipeline — PySpark + Kafka + Airflow

A local batch and streaming data pipeline built on NYC TLC Q1 2024 trip records:
Yellow taxi, Green taxi, and FHV rideshare (Uber/Lyft) — 3 sources, Q1 2024 (Yellow ~10M + Green ~300K + FHV HV ~67M raw rows, ~78M total).
Daily incremental ETL via `TARGET_DATE` env var; orchestrated by Airflow.

## Architecture

### Batch pipeline (01 → 07)

```
data/raw/yellow/        data/raw/green/        data/raw/fhv_hv/
   (Yellow taxi)           (Green taxi)         (Uber / Lyft)
                 \               |               /
                  └──────────────┴──────────────┘
                                 ↓
01_ods_ingest.ipynb      ODS: filter to TARGET_DATE → rename datetime fields
                              → add source_type / ingestion_ts / partition cols
                                 ↓
data/warehouse/ods/      Partitioned by pickup_year / pickup_month / pickup_day
                                 ↓
02_dq_after_ods.ipynb    DQ Gate 1: partition non-empty per source;
                              daily count vs 7-day avg ≤30% — FAIL blocks DWD
                                 ↓
03_dwd_build.ipynb       DWD: unify FHV HV field names → clean → derive cols
                              → star schema (fact_trips + dim_zone + dim_date)
                              dynamic partition overwrite
                                 ↓
data/warehouse/dwd/      fact_trips / dim_zone / dim_date
                                 ↓
04_dq_after_dwd.ipynb    DQ Gate 2: 16 rules — completeness / range / temporal
                              / row conservation / referential integrity
                              FHV HV parity: trip_time, trip_miles, driver_pay,
                              request_datetime order; passenger_count null → tag
                              only (not quarantined); violations → quarantine table
                              FAIL blocks DWS
                                 ↓
05_dws_build.ipynb       DWS: daily zone-level aggregation
                              salt-based two-phase aggregation for hotspot skew
                                 ↓
data/warehouse/dws/      trips_daily (partitioned by year/month/day)
                                 ↓
06_dq_after_dws.ipynb    DQ Gate 3: DWS trip_count ≈ fact_trips ±0.1% — FAIL blocks ADS
                                 ↓
07_ads_metrics.ipynb     ADS: KPI (trip_count, GMV, surcharge_rate, tip_rate,
                              avg_fare, avg_distance, avg_duration)
                                 ↓
data/warehouse/ads/      kpi_daily
```

### Streaming pipeline (08 → 09)

```
data/warehouse/ods/yellow/   (ODS — uncleaned; producer reads ODS so streaming
                              consumer applies its own cleaning, making offline
                              vs streaming reconciliation meaningful)
                                 ↓
08_kafka_producer.ipynb      Replay ODS records → Kafka at 20ms intervals,
                              keyed by PULocationID
                                 ↓
Kafka topic: tlc-trips       3 partitions · localhost:9092 · KRaft mode
                                 ↓
09_spark_streaming.ipynb     5-min event-time tumbling window by PULocationID
                              → foreachBatch: write Parquet + reconcile
                                trip_count vs offline DWD for same window
```

## Tech Stack

- **PySpark 3.5.1** — batch ETL (ODS/DWD/DWS/ADS) and Structured Streaming
- **Kafka 3.7.1** — message broker (KRaft mode, no ZooKeeper)
- **Airflow 2.9.3** — DAG orchestration, retry, backfill (`{{ ds }}` → TARGET_DATE)
- **kafka-python** — producer client
- **pandas + pyarrow** — parquet I/O for Kafka producer

## Notebooks

| # | Notebook | Layer | Description |
|---|---|---|---|
| 01 | `01_ods_ingest.ipynb` | ODS | Ingest Yellow + Green + FHV HV raw parquet → rename datetime fields → add source_type / ingestion_ts / partition cols → write partitioned ODS |
| 02 | `02_dq_after_ods.ipynb` | DQ Gate 1 | Partition non-empty per source; daily count vs 7-day rolling avg ≤30% — FAIL blocks DWD |
| 03 | `03_dwd_build.ipynb` | DWD | Unify FHV HV field names → clean → derive columns → write fact_trips + dim_zone + dim_date; dynamic partition overwrite |
| 04 | `04_dq_after_dwd.ipynb` | DQ Gate 2 | 16 rules: Rules 1–10 Yellow/Green (completeness/range/temporal), Rules 11–12 DWD (true row conservation via DWD_CLEANING / referential integrity), Rules 13–16 FHV HV parity (trip_time/miles/driver_pay/request_after_pickup); passenger_count null tagged not quarantined — FAIL blocks DWS |
| 05 | `05_dws_build.ipynb` | DWS | Daily zone-level aggregation; salt-based two-phase aggregation for hotspot skew |
| 06 | `06_dq_after_dws.ipynb` | DQ Gate 3 | Rule 17: DWS trip_count == fact_trips count within 0.1% tolerance — FAIL blocks ADS |
| 07 | `07_ads_metrics.ipynb` | ADS | KPI: trip_count, GMV, govt_surcharge_rate (government fees / fare), fhv_platform_take_rate (1 - driver_pay / fare, FHV HV only), tip_rate, avg_fare, avg_distance, avg_duration; dynamic partition overwrite |
| 08 | `08_kafka_producer.ipynb` | Stream | Replay ODS yellow records to Kafka at 20ms intervals, keyed by PULocationID |
| 09 | `09_spark_streaming.ipynb` | Stream | Subscribe to tlc-trips → 5-min event-time tumbling window by PULocationID → foreachBatch write Parquet + reconcile vs offline DWD |

## Airflow DAG

`dags/tlc_pipeline.py` — 9 tasks, daily schedule (`0 6 * * *`), `catchup=True`, `end_date=2024-03-31`, retry=2:

```
start → ods_ingest_yellow ─┐
      → ods_ingest_green  ─┼─ dq_after_ods → dwd_build → dq_after_dwd → dws_build → dq_after_dws → ads_metrics → end
      → ods_ingest_fhv    ─┘
```

`{{ ds }}` (logical execution date) is passed as `TARGET_DATE` to each notebook via `env` in the BashOperator. With `catchup=True` and `end_date=2024-03-31`, registering the DAG triggers automatic backfill of all 91 Q1 2024 days. `max_active_runs=1` keeps runs serial so each day's ODS→DWD→ADS completes before the next starts.

Each BashOperator uses `append_env=True` (so `PATH`/`JAVA_HOME` are preserved) and `--output /tmp/<task>_<ds>.ipynb` (so three parallel ODS tasks don't overwrite the same `.ipynb` file).

## Setup

### Prerequisites
- Python 3.10 (conda env)
- Java 11+
- Kafka 3.7.1 (download from kafka.apache.org, extract to `~/kafka/`)

### Install dependencies
```bash
conda create -n tlc-spark python=3.10 -y
conda activate tlc-spark
pip install -r requirements.txt \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.10.txt"
python -m ipykernel install --user --name tlc-spark --display-name "tlc-spark"
```

### Download TLC data
```bash
bash scripts/download_data.sh   # downloads Q1 2024 Yellow + Green + FHV HV (~1.5 GB)
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
export TARGET_DATE=2024-03-15
for nb in 01_ods_ingest 02_dq_after_ods 03_dwd_build 04_dq_after_dwd \
           05_dws_build 06_dq_after_dws 07_ads_metrics; do
  jupyter nbconvert --to notebook --execute --inplace \
    --ExecutePreprocessor.kernel_name=tlc-spark \
    --ExecutePreprocessor.timeout=600 \
    notebooks/${nb}.ipynb
done
```

### Run streaming
```bash
# 1. Start Kafka producer (replays ODS data)
jupyter nbconvert --to notebook --execute --inplace \
  --ExecutePreprocessor.kernel_name=tlc-spark notebooks/08_kafka_producer.ipynb

# 2. Start Spark Structured Streaming consumer
jupyter nbconvert --to notebook --execute --inplace \
  --ExecutePreprocessor.kernel_name=tlc-spark notebooks/09_spark_streaming.ipynb
```

## Data Quality Results (2024-03-15)

### Gate 2 — 16 rules across Yellow/Green and FHV HV

| Rule | Source | Layer | Result |
|---|---|---|---|
| fare_amount not null | Yellow+Green | ODS | ✅ PASS — bad=0 |
| pickup_datetime not null | Yellow+Green | ODS | ✅ PASS — bad=0 |
| PULocationID not null | Yellow+Green | ODS | ✅ PASS — bad=0 |
| DOLocationID not null | Yellow+Green | ODS | ✅ PASS — bad=0 |
| passenger_count null rate < 15% (**tag-only**) | Yellow+Green | ODS | ✅ PASS — 15,302 tagged |
| fare_amount > 0 | Yellow+Green | ODS | ✅ PASS — bad=2,246 (<5%) |
| trip_distance >= 0 | Yellow+Green | ODS | ✅ PASS — bad=0 |
| passenger_count in [1,6] | Yellow+Green | ODS | ✅ PASS — bad=1,515 (<15%) |
| dropoff > pickup | Yellow+Green | ODS | ✅ PASS — bad=39 (<1%) |
| pickup in 2024-Q1 | Yellow+Green | ODS | ✅ PASS — bad=0 |
| trip_time > 0 | FHV HV | ODS | ✅ PASS — bad=0 |
| trip_miles > 0 | FHV HV | ODS | ✅ PASS — bad=100 (<1%) |
| driver_pay >= 0 | FHV HV | ODS | ✅ PASS — bad=6 (<1%) |
| request_datetime ≤ pickup_datetime | FHV HV | ODS | ✅ PASS — bad=7,085 (<1%) |
| fact + dwd_cleaning == ods_all (true conservation, no silent drops) | All | DWD | ✅ PASS |
| PULocationID in dim_zone | All | DWD | ✅ PASS |

**Quarantine rate: 1.27% overall** (Yellow/Green 2.90%, FHV HV 0.98%)

Notable findings:
- `passenger_count` null (11.7% of taxi records) is a known TLC upstream issue; records retain valid fare/time/location → tagged with `passenger_count_missing=true`, not quarantined
- FHV HV `request_after_pickup` (7,085 records): dispatch platform clock sync drift — only detectable after adding FHV HV-specific temporal rules

### Gate 3 — DWS conservation

| Rule | Result |
|---|---|
| Rule 17: DWS trip_count == fact_trips | ✅ PASS — drift 0.0000% |

## Key Metrics (2024-03-15, single-day run)

Numbers reflect the unified dq_reason rules in `03_dwd_build` (including FHV `request_after_pickup`).
Exact Q1-aggregate figures will update after full backfill (`catchup=True`, 91 daily runs).

| Source | Trips (fact) | GMV | Govt Surcharge Rate | FHV Platform Take Rate | Avg Fare |
|---|---|---|---|---|---|
| Yellow | ~121K | ~$3.4M | ~19% | — | ~$19 |
| Green | ~1.9K | ~$43K | ~13% | — | ~$17 |
| FHV HV | ~740K | ~$22M | ~5% | ~28% (1 − driver_pay/fare) | ~$26 |
| **Total** | **~863K** | **~$25.5M** | — | — | — |
