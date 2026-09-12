# Phase 10 — Resilience and dead-letter recovery

## Synchronous reads

Resilience4j circuit breakers protect the cart's catalog lookup and each of the query
service's payment, inventory and shipping lookups independently. A count-based window
holds ten calls; after at least five calls, a failure rate of 50% opens the circuit.
After ten seconds the next caller may make one half-open probe. A successful probe
closes the circuit; a failed probe opens it again. HTTP 4xx responses other than 429
are excluded from the failure calculation. Missing records and business refusals do
not indicate a dependency outage.

Open circuits preserve the existing API contracts: catalog failure returns 503 before
cart mutation; composed details report UNAVAILABLE for that dependency while retaining
healthy sections and the projection. No fabricated price, payment or shipment is used.
The cart limits catalog concurrency to 24 in-flight lookups without a waiting queue;
its existing two-second connect and three-second read timeouts remain. Query retains
its shared one-second deadline and 24-lookup limit from Phase 9. Synchronous calls make
one attempt, avoiding nested retries that would exceed the request deadline.
Checkout HTTP orchestration remains pending; these policies cover the implemented reads.

## Kafka retry and dead-letter policy

All six business consumers use an initial attempt plus at most three retries, with
exponential delay ceilings of 250, 500 and 1000 milliseconds, with each delay
jittered between half its ceiling and the ceiling. Invalid arguments, including
explicit envelope/schema validation rejections, go directly to recovery. Other parsing,
database and listener errors receive the bounded retry budget. Payment declines are
business events, not exceptions to retry. Existing durable payment/refund reconciliation
and outbox recovery remain responsible for their own work.

After exhaustion, the handler publishes the original string key and value to
`<source-topic>.<consumer-service>.DLT`, retaining the source partition. For example,
`order.events.order-query-service.DLT` is separate from
`order.events.inventory-service.DLT`. KafkaAdmin and the repeatable Compose initializer
create the required topics with three partitions and seven-day retention. Replication
factor one remains a local-demo limitation.

Recovery uses `acks=all`, idempotent publication and a bounded wait for the send result.
Only an acknowledged send allows the handler to return successfully and the listener
to advance the source offset. A failed/unknown acknowledgment throws and leaves the
source record for another recovery attempt. A crash after publication but before offset
commit may produce another DLT copy. No DLT is consumed automatically.

Recovery headers contain original topic, partition and offset. Exception messages,
stack traces and inbound headers are not copied. The original payload is retained for
replay: it can include synthetic payment tokens or addresses, so DLTs need the same
access controls as source topics. Payloads are not printed by the redrive tool.
`consumer.dlt.published` and `consumer.dlt.failures` counters are available through the
existing internal metrics endpoint. Watch these alongside consumer lag and buffered
projection counts. A DLT entry is failed work, not a completed business operation.

## Controlled redrive

First fix the consumer, dependency or incompatible schema that caused the failure.
Use Kafka tooling to identify the exact DLT topic, partition and offset. Build the
query service and use Java 21; the launcher obtains Kafka dependencies from its local
packaged jar. The following defaults to a read-only dry run:

```bash
python3 infrastructure/scripts/redrive.py localhost:9092 \
  order.events.order-query-service.DLT 0 12
```

After inspecting the selected position and fixing the cause, explicitly execute it:

```bash
python3 infrastructure/scripts/redrive.py localhost:9092 \
  order.events.order-query-service.DLT 0 12 --execute
```

The tool accepts only the known source/consumer DLT combinations and one exact retained
offset. It never commits a consumer offset or deletes a DLT record. Execution republishes
the original key and payload bytes to the source topic and partition, waits for broker
acknowledgment, and prints the new position. Event ID, version and all payload fields
remain unchanged. It does not repair malformed payloads or manufacture replacement events.
It currently supports the local plaintext demo broker, not production SASL/TLS setup.

Republishing to the source topic intentionally redelivers the event to every subscribed
consumer. Their inbox and producer-version checks suppress completed effects; the failed
consumer can now process its missing event. A projection with gaps may require multiple
positions to be redriven. Rebuilding the projection cannot restore a record that never
entered its journal. Duplicate execution is safe for the implemented idempotent consumers,
but after an unknown publish result inspect downstream state before repeating recovery.

## Verification

Tests cover acknowledgment gating, failed DLT sends, unchanged records, circuit opening,
independent healthy dependencies, half-open recovery and exclusion of 404s. Packaged saga
tests introduce a temporary query-database failure, observe the consumer-specific DLT,
exercise the actual dry-run and execute tool, then check projection recovery and one
charge/notification despite replay. Java 21 `mvn -o verify` passed all 13 reactor
modules: 80 tests with zero failures, errors or skips. Compose configuration, Python
and shell syntax, and whitespace checks passed. This phase has not been deployed to
the shared Compose stack.

The policies use the
documented [Spring Kafka error-handler recovery contract](https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html)
and [Resilience4j circuit-breaker states](https://resilience4j.readme.io/docs/circuitbreaker).
