# Phase 13 — Executable walkthrough and operations

This guide demonstrates the implemented product/cart APIs and the complete simulated
workflow from an accepted order to fulfillment or compensated cancellation. The script
performs an **explicit trusted handoff to order-service**. There is no checkout HTTP
endpoint: durable checkout intents, automatic cart-to-order orchestration and safe
cart clearing remain unimplemented. The script does not substitute for that service.

## Prepare a local deployment

Use Java 21, Maven 3.9+, Python 3 and a running Docker-compatible engine. From the repository
root, generate missing local credentials, run verification and build the current stack:

```bash
python3 infrastructure/scripts/init-env.py
mvn verify
bash infrastructure/scripts/validate.sh
docker compose up -d --build --wait --wait-timeout 300
python3 infrastructure/scripts/verify.py --apps
```

On macOS, select the installed JDK with `export JAVA_HOME=$(/usr/libexec/java_home -v 21)`
if the shell currently uses another version. The environment initializer appends missing
values and preserves existing passwords and the JWT signing key. Do not commit `.env`.
The default gateway is loopback port 8080. See `.env.example` for port overrides.

A stack last deployed before JWT support must be rebuilt before using these commands.
The historic deployment records in the README are not a claim that the latest images
are running. If startup fails, inspect `docker compose ps` and the affected service log.
For a Flyway checksum mismatch, preserve the database and compare the applied migration
with its original source. Restore the original applied migration and express functional
schema changes in a new migration. Do not erase volumes or automatically repair checksums
to bypass unexplained differences.

## Run the walkthrough

The following command creates four independent sets of synthetic products, stock, carts
and append-only orders. It retains them for inspection and prints one JSON summary per
scenario. Each rerun creates new fixtures; the script is not a resumable checkout client.

```bash
python3 infrastructure/scripts/walkthrough.py --scenario all > walkthrough.jsonl
cat walkthrough.jsonl
```

For one outcome, choose `success`, `inventory-rejection`, `payment-decline` or
`shipment-refund`. Use `--gateway http://localhost:18080` for a different gateway, and
`--timeout 120` to change the bounded polling window (1–300 seconds per scenario).

| Scenario | Outcome | Inventory | Payment | Refund |
|---|---|---|---|---|
| success | COMPLETED | RESERVED | COMPLETED | NONE |
| inventory-rejection | CANCELLED | REJECTED | UNKNOWN (no payment record) | NONE |
| payment-decline | CANCELLED | RELEASED | FAILED | NONE |
| shipment-refund | CANCELLED | RELEASED | COMPLETED | REFUNDED |

For each scenario the script:

1. Creates a synthetic product and stock using an admin JWT.
2. Creates and populates a customer-owned cart through the gateway.
3. Reads the current authoritative product price and builds an immutable order snapshot.
4. Submits the snapshot using the trusted checkout identity, repeats the same command/key,
   and verifies the acceptance is unchanged.
5. Waits for the expected projection statuses and notification, checks cross-customer
   rejection and reads composed details from the owning services.
6. Prints scenario, order/product/cart IDs, outcome, trace ID and correlation ID.

The cart remains populated. A real checkout implementation must compare its submitted
version before clearing it so concurrent additions are retained. No real charges,
shipments or customer communications occur; all providers are simulated.

## Inspect a result

Select the first JSON summary and issue a customer token. Commands below use the default
ports; substitute your configured ports as needed.

```bash
ORDER_ID=$(python3 -c 'import json; print(json.loads(open("walkthrough.jsonl").readline())["orderId"])')
TRACE_ID=$(python3 -c 'import json; print(json.loads(open("walkthrough.jsonl").readline())["traceId"])')
TOKEN=$(python3 infrastructure/scripts/demo_token.py --identity customer)
curl --fail-with-body -H "Authorization: Bearer $TOKEN" "http://localhost:8080/api/orders/$ORDER_ID"
curl --fail-with-body -H "Authorization: Bearer $TOKEN" "http://localhost:8080/api/order-views/$ORDER_ID/details"
docker compose exec -T prometheus wget -qO- "http://tempo:3200/api/traces/$TRACE_ID"
unset TOKEN
```

Tempo is internal to the Compose network, so the command queries it from the Prometheus
container. Trace export is asynchronous; allow a short delay before the request. Grafana at
localhost:3000 provides the Checkout Platform dashboard and Tempo datasource. The
business panels distinguish committed outcomes from listener attempts and DLT activity.
A successful HTTP response does not mean every asynchronous service has already caught up.

## Investigate and recover

| Symptom | Read first | Recovery |
|---|---|---|
| Query 404 after acceptance | Order API, query consumer lag and DLT counters | Let normal ingestion catch up; repair the consumer and redrive a missing input if necessary |
| Details section UNAVAILABLE | Owner readiness, logs and circuit-breaker conditions | Restore the owner; later reads/probes recover without changing the order |
| Payment pending/unknown | Payment view, provider ledger and recovery attempts | Let durable reconciliation retry the same identity; never create a replacement charge |
| Order COMPENSATING | Refund state and inventory release facts | Restore the failed dependency and allow existing recovery workers to finish |
| Outbox backlog | Service outbox API, broker availability and publication failures | Restore broker access; existing rows retry with stable event IDs |
| DLT entry | Source topic/partition/offset and consumer failure | Fix the cause, dry-run the exact DLT position, then explicitly redrive it |
| Incorrect/missing projection | Journal, buffered events and generation status | Redrive missing inputs first; rebuild only data already present in the journal |

Read-only admin diagnostics for an order:

```bash
ADMIN_TOKEN=$(python3 infrastructure/scripts/demo_token.py --identity admin)
curl --fail-with-body -H "Authorization: Bearer $ADMIN_TOKEN" "http://localhost:8080/api/orders/$ORDER_ID/events"
curl --fail-with-body -H "Authorization: Bearer $ADMIN_TOKEN" "http://localhost:8080/api/orders/$ORDER_ID/outbox"
curl --fail-with-body -H "Authorization: Bearer $ADMIN_TOKEN" "http://localhost:8080/api/payments/$ORDER_ID/provider"
curl --fail-with-body -H "Authorization: Bearer $ADMIN_TOKEN" "http://localhost:8080/api/payments/$ORDER_ID/refund"
curl --fail-with-body -H "Authorization: Bearer $ADMIN_TOKEN" http://localhost:8080/api/order-views/admin/projection
```

A payment/refund diagnostic may legitimately return 404 when that branch never ran.
`POST /api/payments/{orderId}/retry` only expedites eligible existing work; it does not
authorize a new payment or override a decline. See [payment recovery](phase-6-payment.md).

To rebuild the projection after diagnosing the problem:

```bash
curl --fail-with-body -X POST -H "Authorization: Bearer $ADMIN_TOKEN" \
  http://localhost:8080/api/order-views/admin/rebuild
curl --fail-with-body -H "Authorization: Bearer $ADMIN_TOKEN" \
  http://localhost:8080/api/order-views/admin/projection
unset ADMIN_TOKEN
```

The POST returns 202 and the new generation (409 if a build is already running). Poll
status until buildingGeneration is null and activeGeneration equals the requested
generation. If it does not switch, inspect logs: a failed build leaves the prior
active generation available. A rebuild cannot recover inputs absent from the journal.

For an identified DLT position, use [Phase 10's redrive procedure](phase-10-resilience.md).
The tool defaults to a dry run and preserves the original event key/payload on execution.
Replaying on the source topic delivers to all subscribers; inbox/version checks suppress
completed effects. Inspect state before repeating a command whose acknowledgment was lost.

Use `docker compose stop` to pause the stack while preserving volumes. Use the same `.env`
and volumes when resuming with `docker compose up -d --wait`. Never use `down -v` as a
routine recovery step for append-only business data.

## Verification and remaining scope

The packaged saga suite executes this exact Python script for all four outcomes using
isolated application jars, PostgreSQL and Kafka. It also checks one OrderCreated event,
one command record, one notification and the expected provider charge count per outcome.
The existing suite covers DLT/redrive, rebuilds, restart recovery, concurrency, JWT and
actual exported trace relationships. See [the coverage map](phase-12-verification.md).

On 2026-09-12, Java 21 `mvn -o verify` passed all 13 reactor modules: 90 tests,
zero failures, errors or skips, including all 13 packaged saga tests and all four
walkthrough outcomes. Compose configuration, shell/Python syntax, dashboard JSON
and whitespace checks passed. The shared Compose stack has not been rebuilt or
populated by this phase. Remaining product scope includes checkout HTTP
orchestration, production identity-provider integration and production-grade durability,
retention and load validation. The documented milestones do not imply those are complete.
