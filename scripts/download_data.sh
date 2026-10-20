#!/usr/bin/env bash
# Download NYC TLC trip data for Q1 2024 (~1.5 GB total)
set -euo pipefail

BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"
mkdir -p data/raw/yellow data/raw/green data/raw/fhv_hv

# Taxi zone lookup (required by 03_dwd_build to build dim_zone)
curl -# -o data/raw/taxi_zone_lookup.csv \
  "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

for month in 01 02 03; do
  curl -# -o "data/raw/yellow/yellow_tripdata_2024-${month}.parquet" \
    "${BASE_URL}/yellow_tripdata_2024-${month}.parquet"
  curl -# -o "data/raw/green/green_tripdata_2024-${month}.parquet" \
    "${BASE_URL}/green_tripdata_2024-${month}.parquet"
  curl -# -o "data/raw/fhv_hv/fhvhv_tripdata_2024-${month}.parquet" \
    "${BASE_URL}/fhvhv_tripdata_2024-${month}.parquet"
done

echo "Download complete. Sizes:"
du -sh data/raw/yellow data/raw/green data/raw/fhv_hv
