# TLC Trip Data Pipeline — PySpark + Kafka

A local batch and streaming data pipeline built on NYC TLC Yellow Taxi trip records (Q1 2024, 8.4M rows).

## Architecture

```
data/raw/*.parquet          (NYC TLC source data)
       ↓
01_batch_etl.ipynb          Spark batch ETL: clean → transform → partitioned parquet
       ↓
data/processed/trips/       Hive-style partitioned output (pickup_year / pickup_month)
       ↓
02_kafka_producer.ipynb     Kafka producer: replay processed records as a real-time stream
       ↓
Kafka broker (tlc-trips)    3-partition topic on localhost:9092
       ↓
03_spark_streaming.ipynb    Spark Structured Streaming: 30s window aggregations
```

## Tech Stack

- **PySpark 3.5.1** — batch ETL and structured streaming
- **Kafka 3.7.1** — message broker (KRaft mode, no Zookeeper)
- **kafka-python** — producer client
- **pandas + pyarrow** — parquet I/O for producer

## Notebooks

| Notebook | Description |
|---|---|
| `01_batch_etl.ipynb` | Ingest raw TLC parquet → clean (11.3% invalid rows filtered) → add derived columns (trip duration, speed, time-of-day) → write partitioned parquet |
| `02_kafka_producer.ipynb` | Read processed parquet → publish records to Kafka topic `tlc-trips` at 50ms intervals |
| `03_spark_streaming.ipynb` | Subscribe to `tlc-trips` → parse JSON → 30-second tumbling window aggregations → console sink |

## Setup

### Prerequisites
- Python 3.10 (conda env)
- Java 11+
- Kafka 3.7.1 (download from kafka.apache.org)

### Install dependencies
```bash
conda create -n tlc-spark python=3.10 -y
conda activate tlc-spark
pip install pyspark==3.5.1 kafka-python pandas pyarrow jupyter ipykernel
python -m ipykernel install --user --name tlc-spark --display-name "tlc-spark"
```

### Download TLC data
```bash
mkdir -p data/raw
cd data/raw
curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet
curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet
```

### Start Kafka
```bash
# First time (or after clearing ~/kafka-data)
~/kafka/bin/kafka-storage.sh format \
  -t $(~/kafka/bin/kafka-storage.sh random-uuid) \
  -c ~/kafka/config/kraft/server.properties

# Start broker
~/kafka/bin/kafka-server-start.sh ~/kafka/config/kraft/server.properties

# Create topic (new terminal)
~/kafka/bin/kafka-topics.sh --create \
  --topic tlc-trips \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### Run
1. Open `notebooks/01_batch_etl.ipynb` → run all cells (kernel: tlc-spark)
2. Open `notebooks/02_kafka_producer.ipynb` → run all cells
3. Open `notebooks/03_spark_streaming.ipynb` → run cells 1–4, then cell 5; switch to notebook 02 and re-run the producer cell to see live results

## Data Quality

Raw data issues found and handled:

| Issue | Rows affected | Fix |
|---|---|---|
| Null passenger_count | 751,962 | Filter: `between(1, 6)` |
| Negative fare_amount | ~few thousand | Filter: `fare_amount > 0` |
| Wrong-year timestamps (2002, 2008, 2009) | ~handful | Filter: `pickup_datetime >= 2024-01-01` |
| April records in March file | ~few hundred | Filter: `pickup_datetime < 2024-04-01` |
| Extreme trip_distance (312,722 miles) | ~few | Indirect: `avg_speed_mph < 100` |

Total removed: **1,075,366 rows (11.3%)**
