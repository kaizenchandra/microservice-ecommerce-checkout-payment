#!/usr/bin/env bash
set -euo pipefail
# Safe to repeat. Dead-letter topics retain the source partition and consumer identity.
for topic in order.events inventory.events payment.events shipping.events notification.events infrastructure.smoke; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists \
    --topic "$topic" --partitions 3 --replication-factor 1 \
    --config min.insync.replicas=1 --config retention.ms=604800000
 done

for mapping in order-service:inventory.events order-service:payment.events order-service:shipping.events inventory-service:order.events inventory-service:payment.events payment-service:inventory.events payment-service:shipping.events shipping-service:payment.events notification-service:order.events order-query-service:order.events order-query-service:inventory.events order-query-service:payment.events order-query-service:shipping.events order-query-service:notification.events; do
  consumer="${mapping%%:*}"
  source_topic="${mapping#*:}"
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists \
    --topic "${source_topic}.${consumer}.DLT" --partitions 3 --replication-factor 1 \
    --config min.insync.replicas=1 --config retention.ms=604800000
done
