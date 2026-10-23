# TLC Trip Data Pipeline — PySpark + Kafka + Airflow

**[→ Live Dashboard](https://jellyiilu.github.io/tlc-spark-pipeline)**

A local batch and streaming data pipeline built on NYC TLC Q1 2024 trip records:
Yellow taxi, Green taxi, and FHV rideshare (Uber/Lyft) — 3 sources, Q1 2024 (~70.0M raw rows: Yellow ~9.55M + Green ~0.17M + FHV HV ~60.30M).
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
                              (pickup_date × PULocationID × source_type)
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
08_kafka_producer.ipynb      Replay one hour of ODS records in event-time order
                              (+ heartbeat) → Kafka, keyed by PULocationID
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
| 05 | `05_dws_build.ipynb` | DWS | Daily zone-level aggregation by pickup_date × PULocationID × source_type |
| 06 | `06_dq_after_dws.ipynb` | DQ Gate 3 | Rule 17: DWS trip_count == fact_trips count within 0.1% tolerance — FAIL blocks ADS |
| 07 | `07_ads_metrics.ipynb` | ADS | KPI: trip_count, GMV, govt_surcharge_rate (government fees / fare), fhv_platform_take_rate (1 - driver_pay / fare, FHV HV only), tip_rate, avg_fare, avg_distance, avg_duration; dynamic partition overwrite |
| 07b | `07b_ads_borough.ipynb` | ADS | Full Q1 recompute (no date partition): DWS × dim_zone × dim_date via broadcast join → kpi_by_borough with 6 additive columns (trip_count, gmv, fare_sum, tip_sum, distance_sum, duration_sum) + derived avg_fare/tip_rate/avg_miles/avg_min; conservation check asserts sum(trip_count) unchanged; borough Unknown/N/A = TLC reserved IDs 264–265 |
| 08 | `08_kafka_producer.ipynb` | Stream | Replay one hour of ODS yellow records in event-time order (+ watermark heartbeat) → Kafka, keyed by PULocationID |
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
# 1. Reset: recreate Kafka topic and clear output/checkpoint
bash scripts/reset_stream.sh

# 2. Terminal A: start consumer (waits for data, auto-stops 30s after last record)
jupyter nbconvert --to notebook --execute --inplace \
  --ExecutePreprocessor.kernel_name=tlc-spark \
  --ExecutePreprocessor.timeout=1200 \
  notebooks/09_spark_streaming.ipynb

# 3. Terminal B (~30s later, once consumer is ready): start producer
jupyter nbconvert --to notebook --execute --inplace \
  --ExecutePreprocessor.kernel_name=tlc-spark notebooks/08_kafka_producer.ipynb
```

## Results (Q1 2024, 91-day backfill)

### Exported CSVs (`results/`)

| File | Rows | Contents |
|---|---|---|
| `results/kpi_daily.csv` | 273 | ADS daily KPIs (trip_count, GMV, rates) by pickup_date × source_type |
| `results/kpi_by_borough.csv` | 55 | ADS borough KPIs by borough × service_zone × day_type × source_type |
| `results/dq_rule_summary.csv` | 23 | DQ rule stats per gate × rule_id × severity: n / pass / warn / fail / skip / bad_rate min–max |

### Layer reconciliation — 91 days × 3 sources, zero drift

`check_layers.py` verifies per (day × source): `ODS = fact + rejected`, `fact = DWS`, `DWS = ADS`.

| Source | ODS | fact_trips | Rejected | Reject rate |
|---|---|---|---|---|
| FHV HV | 60,303,866 | 60,271,974 | 31,892 | 0.05% |
| Yellow | 9,554,722 | 9,075,665 | 479,057 | 5.01% |
| Green | 167,567 | 154,715 | 12,852 | 7.67% |
| **Total** | **70,026,155** | **69,502,354** | **523,801** | **0.75%** |

Top rejection reasons (yellow): zero distance 42.6%, non-positive fare (refunds/voids) 29.1%, invalid passenger_count 21.4%.

### Data quality — results persisted to `dq/dq_results` (one row per rule per day)

- 0 blocking failures across 91 days; 14 warnings
- **Rule 16** (FHV request_datetime > pickup_datetime): 0.40%–1.54% daily — systematic upstream clock drift. The original 1% blocking threshold would have halted **54 / 91 days**; changed to tag-in-fact + warn at 3%
- **Rule 5** (taxi passenger_count null rate): monthly avg 4.8% → 6.0% → **11.6%** (Jan→Mar), all 5 warnings in late March — a rising upstream trend, only visible because DQ results are persisted
- **Rule 0b** (volume vs same weekday last week): warnings line up with holidays and the 2024-02-13 snowstorm; the anomaly "echoes" one week later (02-20) — next step: use a 4-week same-weekday median as baseline

### Key metrics (ADS, weighted sum/sum)

| Source | Trips | GMV | Avg fare | Tip rate | Platform take rate |
|---|---|---|---|---|---|
| FHV HV | 60.27M | $1,859M | $24.62 | 4.4% | 22.9% |
| Yellow | 9.08M | $250M | $18.70 | 17.9%* | — |
| Green | 0.15M | $3.5M | $16.76 | 14.3%* | — |

\* Taxi cash tips are not recorded. Platform take rate = 1 − driver_pay / base_passenger_fare.

### Streaming vs offline reconciliation (Kafka + Spark 3.5.1)

Replayed yellow-taxi ODS records for 2024-03-15 08:00–09:00 into Kafka (3 partitions, keyed by PULocationID); Structured Streaming applied the same cleaning rules as DWD and aggregated 5-min event-time windows × pickup zone.

| Metric | Value |
|---|---|
| Records replayed | 5,246 (+1 watermark heartbeat) |
| After cleaning (stream) | 4,920 |
| Offline DWD, same window | 4,920 |
| (window × zone) cells compared | 835 (12 windows) |
| Mismatched / duplicate cells | 0 / 0 |

- Cleaning is a verbatim copy of DWD's `when`-chain + `dq_reason IS NULL`; using `filter(fare > 0)` instead would diverge on NULLs (three-valued logic)
- A heartbeat record (event time = END + 10 min) advances the watermark so the last window closes. It must **not** be filtered before aggregation: the optimizer pushes the filter below `EventTimeWatermark`, so the heartbeat never reaches the watermark. It is aggregated into virtual zone `-1` and excluded in `foreachBatch`
- Run order: `bash scripts/reset_stream.sh` → start 09 → run 08

### Borough KPI (ADS, Q1 2024, `ads/kpi_by_borough`)

55 rows × (borough, service_zone, day_type, source_type); 0 unmatched location IDs; `sum(trip_count)` conservation check passed (69,502,354 in = 69,502,354 out).

| Borough | Trips | GMV |
|---|---|---|
| Manhattan | 32.1M | $1,044M |
| Brooklyn | 16.0M | $408M |
| Queens | 13.0M | $470M |
| Bronx | 7.5M | $167M |
| Staten Island | 0.88M | $22M |
| Unknown | 27,522 | $0.8M |
| N/A | 3,836 | $0.2M |
| EWR | 72 | $6K |

`Unknown` (location_id 264) = location unknown; `N/A` (location_id 265, zone "Outside of NYC") = pickup outside NYC boundaries; `EWR` (location_id 1) is Newark Airport; none are data quality issues.

Both dimension joins use `F.broadcast()` (dim_zone 265 rows, dim_date 91 rows) to avoid shuffle.

### Known limitations
- `passenger_count = 0` (yellow ~102K rows) is rejected while NULL is only tagged — inconsistent, kept as a conservative choice
- DWD selects the intersection of columns across sources, dropping `hvfhs_license_num` (Uber vs Lyft) and taxi `extra`
- Airflow backfill on macOS: task processes occasionally hung at fork (6 times in 91 days); a local watchdog script detected and restarted stuck runs
