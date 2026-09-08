# Phase 4 — Order, event sourcing and transactional outbox

Order now accepts a trusted priced snapshot, reconstructs every read from persisted
events, and publishes those events to `order.events`. The gateway routes `/api/orders`.
Checkout orchestration, inventory/payment/shipping consumers and terminal order states
belong to later phases. An accepted order remains `PENDING` in this phase.

## Run and inspect

```bash
python3 infrastructure/scripts/init-env.py
docker compose up -d --build --wait --wait-timeout 300 order-service api-gateway
set -a
source .env
set +a
ORDER_ID=$(uuidgen | tr '[:upper:]' '[:lower:]')
COMMAND_ID=$(uuidgen | tr '[:upper:]' '[:lower:]')
CUSTOMER=11111111-1111-1111-1111-111111111111
cat > /tmp/demo-order.json <<JSON
{
  "orderId": "$ORDER_ID",
  "customerId": "$CUSTOMER",
  "cartId": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
  "cartVersion": 1,
  "items": [{
    "productId": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1",
    "sku": "DEMO-1", "name": "Demo item", "quantity": 2,
    "unitPrice": {"amount": 12.50, "currency": "USD"}
  }],
  "paymentToken": "tok_success",
  "shippingAddress": {
    "recipient": "Demo Buyer", "line1": "1 Test Street", "city": "Test City",
    "postalCode": "12345", "country": "US"
  }
}
JSON
curl --fail-with-body -i -u "checkout:$DEMO_CHECKOUT_PASSWORD" \
  -H 'Content-Type: application/json' -H "Idempotency-Key: $COMMAND_ID" \
  --data-binary @/tmp/demo-order.json http://localhost:8080/api/orders
curl --fail-with-body -u "$CUSTOMER:$DEMO_CUSTOMER_PASSWORD" \
  "http://localhost:8080/api/orders/$ORDER_ID"
curl --fail-with-body -u "$CUSTOMER:$DEMO_CUSTOMER_PASSWORD" \
  -H 'Content-Type: application/json' \
  -d '{"expectedVersion":1,"note":"Synthetic demo note"}' \
  "http://localhost:8080/api/orders/$ORDER_ID/notes"
curl --fail-with-body -u "admin:$DEMO_ADMIN_PASSWORD" \
  "http://localhost:8080/api/orders/$ORDER_ID/events"
curl --fail-with-body -u "admin:$DEMO_ADMIN_PASSWORD" \
  "http://localhost:8080/api/orders/$ORDER_ID/outbox"
```

Creation returns 202 and `Location`, version 1 and USD 25.00. Repeat the exact create
request with the same key to retrieve the original acceptance without another event.
Changing the body with that key returns 409. Notes append a second event and increment
the reconstructed version. Repeating the version-1 note returns 409; reload before
submitting another note. Notes are bounded to 20 per order, 500 characters each, and do
not change fulfillment instructions. Note commands do not have retry idempotency keys.

Only CHECKOUT and ADMIN can create orders. The customer ID in this internal command is
trusted because of that role restriction; customers cannot choose their own prices.
The future checkout implementation must fetch authoritative catalog prices. These demo
commands deliberately accept synthetic snapshots without live catalog/cart calls.
Customers can read and annotate only their own orders; another customer's ID returns
404. Event history and delivery diagnostics require ADMIN. JWT arrives in Phase 11.

## Persistence and reconstruction

`order_stream` is a technical version cursor. `domain_event` is the source of business
state; there is no mutable order status or total table. Each append compares the expected
version, then writes the event and matching outbox row in the same local transaction.
Unique stream/version and event-ID constraints provide additional protection. Database
triggers reject rewriting or deleting history and changing outbox identity or payload.

The create transaction also stores a customer/key-scoped command hash and original
response. Concurrent inserts use PostgreSQL conflict handling. Items are sorted by
product ID, monetary scale is normalized and the default sales channel is applied before
hashing. Reuse with different semantic content conflicts. A different key cannot reuse an
existing order ID. Failed transactions retain none of the command, cursor, event or outbox.

GET reconstructs from ordered events each time, checking aggregate identity and contiguous
versions. Restarting the process does not require rebuilding a cache. The admin history
endpoint is a per-order export for inspection. Bulk projection rebuild is Phase 8.
Schema-1 fixtures under `order-service/src/test/resources/contracts` verify additive
`salesChannel` compatibility: old events default to WEB and old readers ignore the new
field. Unsupported schema versions fail closed; breaking changes need explicit upcasters.

## Publication and failure behavior

The poller locks only the oldest pending event of each stream with `FOR UPDATE SKIP LOCKED`.
Another poller can progress a different stream, but cannot overtake a locked or delayed
predecessor. Each publication has its own transaction, a bounded Kafka acknowledgement
wait, and a bounded batch. Kafka records use the order UUID as key and carry event,
correlation and causation headers, plus optional traceparent.

A send failure leaves the row pending with exponential backoff and jitter, capped at
about 60 seconds. An acknowledgement permits the PUBLISHED mark. A process failure after
acknowledgement but before the database commit republishes the **same event ID and payload**.
This is at-least-once delivery; later consumers must persist inbox deduplication with their
business transaction. Kafka producer idempotence does not close this database crash window.

For a local outage experiment, stop Kafka, create a fresh order, and inspect its outbox;
then start Kafka and wait for the retry to mark it published. Existing writes/reads depend
on PostgreSQL, so broker failure does not prevent acceptance. Avoid this experiment when
other work needs the shared broker. Never delete a blocked event to let successors pass.
Unsupported/corrupt events require an operator fix to the reader; original history remains
unchanged. Monitor `outbox.pending`, `outbox.oldest.seconds`, `outbox.publish.failures`,
`outbox.published`, and `orders.created` through Actuator/Prometheus. Gauges refresh after
polling; if the poll itself fails, the last gauge values can be stale.

Only synthetic addresses, notes and whitelisted fake payment tokens are supported. The
current educational OrderCreated payload includes the fake token in persisted history
and the domain topic; it is not a design for distributing real payment credentials.
Before real customer use, separate payment-only data, define topic ACLs and a PII retention
policy. Customer views omit the token; operational errors omit payloads.

## Verification

```bash
./mvnw -pl order-service -am verify
./mvnw clean verify
```

Tests use isolated PostgreSQL 17 and Kafka containers. They exercise REST authorization,
validation, replay, concurrent idempotency, expected-version conflicts, atomic rollback,
append-only protections, actual Kafka records, failed sends, delayed predecessors,
competing pollers and the acknowledgement/rollback duplicate window. Fault injection
uses a controlled KafkaTemplate with real PostgreSQL transactions; actual broker delivery
is tested separately. The full saga and CQRS projection are intentionally later phases.

For a repeatable check of a running Compose stack, run
`python3 infrastructure/scripts/verify-orders.py`. It retains one synthetic order and its
two immutable events. After user approval, this script passed against the source-built
Compose gateway, including Kafka acknowledgement for both events. The retained order is
`4d124df9-8bee-48ec-97db-bdac907d4fdb`.
The Java 21 full reactor passed 33 tests with no failures, errors or skips, including all
ten order tests. Source-built order/gateway readiness and read-only routing checks passed.
