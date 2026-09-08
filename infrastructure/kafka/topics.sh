#!/usr/bin/env bash
set -euo pipefail
# Safe to repeat. Consumer-specific retry/DLT topics are added with Phase 10 policies.
for topic in order.events inventory.events payment.events shipping.events notification.events infrastructure.smoke; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists \
    --topic "$topic" --partitions 3 --replication-factor 1 \
    --config min.insync.replicas=1 --config retention.ms=604800000
 done
