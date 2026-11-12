#!/usr/bin/env bash
# Create the single input topic. Partition count and key choice are discussed
# in the README under "Design decisions". This script is the one place that
# actually sets the partition count.
set -euo pipefail

bootstrap="${KAFKA_BOOTSTRAP_SERVERS:-kafka:19092}"
topic="${TOPIC:-shop.events}"
partitions="${TOPIC_PARTITIONS:-3}"
retention_ms="${TOPIC_RETENTION_MS:-86400000}"

echo "waiting for broker at ${bootstrap}"
ready=0
for _ in $(seq 1 30); do
  # timeout so a blackholed broker does not pin this one-shot container.
  if timeout 15 /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${bootstrap}" --list >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 2
done
if [[ "${ready}" -ne 1 ]]; then
  echo "broker not ready: ${bootstrap}" >&2
  exit 1
fi

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${bootstrap}" \
  --create --if-not-exists \
  --topic "${topic}" \
  --partitions "${partitions}" \
  --replication-factor 1 \
  --config "retention.ms=${retention_ms}" \
  --config cleanup.policy=delete

echo "topic ready:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "${bootstrap}" --describe --topic "${topic}"
