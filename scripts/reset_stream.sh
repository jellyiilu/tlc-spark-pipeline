#!/usr/bin/env bash
# 重置实时链路：重建 Kafka topic（清掉残留消息），清空流式输出与 checkpoint
set -euo pipefail
KAFKA="${KAFKA_HOME:-$HOME/kafka}"
BS="localhost:9092"
TOPIC="tlc-trips"
cd "$(dirname "$0")/.."

"$KAFKA/bin/kafka-topics.sh" --bootstrap-server "$BS" --delete --topic "$TOPIC" 2>/dev/null || true
for _ in $(seq 1 30); do
  "$KAFKA/bin/kafka-topics.sh" --bootstrap-server "$BS" --list | grep -qx "$TOPIC" || break
  sleep 1
done
"$KAFKA/bin/kafka-topics.sh" --bootstrap-server "$BS" --create --topic "$TOPIC" \
  --partitions 3 --replication-factor 1
rm -rf data/streaming
echo "Reset done: topic $TOPIC recreated (3 partitions), data/streaming cleared."
