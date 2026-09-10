# Phase 6 — Payment simulator, persisted idempotency and recovery

The stage-specific behavior below is extended by [Phase 7](phase-7-saga.md), which now
implements order outcomes, automatic compensation, refunds, shipping and notifications.

Payment consumes `InventoryReserved`, persists a payment intent, and uses a durable
simulated provider to obtain a charge result. Completion and decline are published through
the payment outbox. The payment ID is the order UUID and is the stable provider idempotency
key across duplicate events, retries, worker crashes and application restarts.

This phase does not move money. Refunds, shipping, automatic inventory compensation and
order status consumers belong to Phase 7. A PaymentFailed event does not yet release stock;
Order remains PENDING until the remaining choreography is implemented.

## Durable processing boundaries

1. The Kafka listener validates the inventory envelope and key, then commits a
   `processed_event` marker and a PENDING payment intent together. Only then can Kafka
   acknowledge the input. Duplicate event IDs are no-ops; different event IDs for the same
   order also reuse the intent. Changed instructions conflict and roll back their marker.
2. A worker claims one due payment in a short transaction using `FOR UPDATE SKIP LOCKED`.
   It persists UNKNOWN, an expiring lease token and an attempt count before contacting the
   provider. A crash before the call is therefore recoverable, just like a crash after it.
3. Outside the business transaction, the worker looks up the provider result by stable
   payment ID. It submits a charge with that same key only if the simulator confirms no
   terminal result. Provider unavailability is an exception, never a fabricated absence.
4. A fresh transaction compares the lease token, stores the terminal payment outcome and
   inserts exactly one outbox event. An expired worker whose lease was replaced cannot
   finalize or defer a newer worker's attempt. The provider independently deduplicates
   concurrent calls, so lease expiry does not create a second charge effect.
5. The existing ordered outbox pattern waits for Kafka acknowledgement, then marks the
   row published. A crash between these steps can deliver the same event twice. Downstream
   consumers must persist their own inbox deduplication with their business changes.

The simulator ledger (`provider_charge`) deliberately commits independently of application
payment state. It uses a separate transactional bean with REQUIRES_NEW, and the simulator
adapter rejects calls inside an active business transaction. A unique payment key and
locked ledger row preserve one successful charge effect. Ledger request hashing rejects
changed amounts or tokens under an existing key. The ledger has no FK to application
payments and never reads the application payment table.

Both ledgers live in payment_db for this educational simulator, so they share a physical
failure domain. An actual external provider adapter must supply idempotent charge and
reliable reconciliation semantics with bounded network calls. An eventually consistent
"not found" response is not sufficient evidence to switch keys or charge again.

## Deterministic simulator outcomes

| Fake token   | Simulator behavior                                                                 | Application result                           |
|--------------|------------------------------------------------------------------------------------|----------------------------------------------|
| tok_success  | Persist a successful charge and return it                                          | COMPLETED + PaymentCompleted                 |
| tok_declined | Persist a terminal decline, with no charge effect                                  | FAILED + PaymentFailed                       |
| tok_timeout  | Persist success, then lose the response after commit                               | UNKNOWN, then COMPLETED after reconciliation |
| tok_error    | First two charge calls report temporary unavailability; the third persists success | UNKNOWN during retries, then COMPLETED       |

The token whitelist is synthetic. Tokens never appear in payment REST views, outgoing
payment events or operational error messages. PaymentCompleted includes the customer,
amount, product/quantity lines and synthetic shipping address required by the future
shipment consumer. PaymentFailed includes the customer, amount and reason DECLINED.

UNKNOWN is neither a decline nor a compensation signal. It emits no terminal event. A
lost reply or database failure after the provider commits leaves recovery work in durable
storage. Recovery checks the ledger and finalizes the existing result without charging
again. The provider diagnostics distinguish submitted charge `requests` from successful
`chargeCount` effects (zero or one). Reconciliation queries do not increment requests.

## Recovery and ordering

The poller uses bounded batches of ten by default. Each attempt has a 30-second lease.
Failures clear the current lease and persist exponential backoff with jitter, capped at
about 60 seconds. A process that disappears leaves its lease to expire. Restarting the
application resumes due PENDING/UNKNOWN rows. There is no in-memory-only retry queue.

`PAYMENT_RECOVERY_ENABLED=false` pauses recovery while keeping accepted intents durable.
`PAYMENT_LISTENER_ENABLED=false` pauses ingestion, and `OUTBOX_ENABLED=false` pauses
publication independently. YAML settings expose the polling interval, bounded batch size
and lease length. Terminal charge state and original payment inputs are immutable through
database triggers. An ADMIN retry request only expedites an unleased pending/unknown
intent; it does not reset a terminal result or take over an active lease.

The consumer group is `payment-charges-v1` with auto-commit disabled, record acknowledgements
and earliest retained offsets for a new group. InventoryReservationFailed and
InventoryReleased are known unrelated facts and ignored in this phase. Unsupported types,
invalid schemas and changed identities fail without accepting a payment. Until Phase 10
adds DLT/redrive, the configured Kafka error handler retries failures without a successful
recovery/offset commit; a poison record can block its partition.

Payment envelope aggregate type is Payment and aggregate ID is the payment/order UUID.
Its terminal charge event has aggregateVersion 1 and schemaVersion 1. Correlation ID and
traceparent are carried from InventoryReserved; causationId is that inventory event's ID.
These versions are payment-local, independent of inventory and order versions.

Metrics include `payments.accepted`, `payments.completed`, `payments.declined`,
`payments.recovery.deferred`, `payments.unresolved`, `payments.unresolved.oldest.seconds`,
and service-tagged `outbox.*`. Gauges update after successful polls and can be stale if
polling fails. IDs are not metric labels.

## Run and inspect

```bash
python3 infrastructure/scripts/init-env.py
docker compose up -d --build --wait --wait-timeout 300 payment-service api-gateway
set -a
source .env
set +a
```

Starting the new group consumes retained inventory events and may create payment intents
and simulated charge outcomes. Use the [Phase 4 order example](phase-4-order-outbox.md)
with a stocked product from [Phase 5](phase-5-inventory.md) and one of the fake tokens above.
Then inspect that order's payment through the gateway:

```bash
curl --fail-with-body -u "11111111-1111-1111-1111-111111111111:$DEMO_CUSTOMER_PASSWORD" \
  "http://localhost:8080/api/payments/$ORDER_ID"
curl --fail-with-body -u "admin:$DEMO_ADMIN_PASSWORD" \
  "http://localhost:8080/api/payments/$ORDER_ID/provider"
curl --fail-with-body -u "admin:$DEMO_ADMIN_PASSWORD" \
  "http://localhost:8080/api/payments/$ORDER_ID/outbox"
curl --fail-with-body -X POST -u "admin:$DEMO_ADMIN_PASSWORD" \
  "http://localhost:8080/api/payments/$ORDER_ID/retry"
```

Payment can initially return 404 while upstream events are pending. Provider diagnostics
return 404 until the simulator has a ledger entry. A customer can read only their own
payment, with another customer's ID returning 404. Provider/outbox diagnostics and retry
are ADMIN-only. The local Basic-auth adapter follows earlier phases; JWT arrives in
Phase 11. No public charge endpoint trusts a customer-supplied amount or token.

## Verification

```bash
./mvnw -pl payment-service -am verify
./mvnw clean verify
```

The suite uses isolated PostgreSQL 17 and Kafka containers, with Flyway and real REST,
listener and producer configuration. It covers success/decline, response loss, transient
provider failure, changed instructions, concurrent input deduplication, concurrent provider
calls, expired-lease fencing, atomic input acceptance, provider-success/application-rollback,
ownership, schema validation and application restart with automatic recovery. A real Kafka
publish followed by database rollback verifies duplicate delivery with stable event identity.
No real provider or customer payment data is involved.

On 2026-09-09, the full Java 21 reactor passed 52 tests with no failures, errors or skips,
including all twelve payment tests. Compose configuration and whitespace checks passed.
After explicit user approval, the source-built payment service and gateway were deployed
and reported healthy. Read-only gateway checks verified authentication, ADMIN-only provider
diagnostics and missing-payment responses. The payment consumer processed the retained
InventoryReservationFailed event and reached zero lag on its nonempty partition; the other
two partitions were empty. The rejected synthetic order correctly has no payment. Successful
charges, declines and recovery were verified in the isolated PostgreSQL/Kafka suite.
