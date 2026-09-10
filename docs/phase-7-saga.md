# Phase 7 — Choreography, shipping, notifications and compensation

Order, Inventory, Payment, Shipping and Notification now cooperate through domain facts.
No service calls the next service to direct the workflow. Order observes facts, records
its own event history and publishes a terminal outcome only after its prerequisites hold.
Checkout HTTP orchestration and the CQRS query projection are still separate work.

## Complete workflows

| Trigger                                                    | Resulting facts and final order outcome                                                               |
|------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| Stock available, successful payment, supported destination | InventoryReserved → PaymentCompleted → ShipmentCreated → OrderCompleted → CustomerNotified            |
| Missing or insufficient stock                              | InventoryReservationFailed → OrderCancelled → CustomerNotified; no charge                             |
| Terminal payment decline                                   | PaymentFailed → InventoryReleased → OrderCancelled → CustomerNotified                                 |
| Shipping rejection after payment                           | ShipmentFailed → PaymentRefunded → InventoryReleased → OrderCancelled → CustomerNotified              |
| Charge or refund response loss                             | Retain UNKNOWN/recovery work; reconcile the durable provider effect before emitting completion/refund |

Shipping maps PaymentCompleted into a local contract, records one shipment outcome per
order and writes its outbox atomically with input deduplication. Tracking references are
synthetic and deterministic (`DEMO-<order UUID>`). Country `ZZ` intentionally produces
ShipmentFailed with UNSUPPORTED_DESTINATION. Other bounded synthetic addresses produce
ShipmentCreated. This is shipment creation, not a physical dispatch/delivery simulator;
successful orders keep inventory allocated in their reservation.

Notification reacts only to OrderCompleted/OrderCancelled. Its durable delivery log records
one terminal order notification per order and emits CustomerNotified via an outbox in the
same transaction. No email, SMS or external message is sent. Duplicate event IDs and
semantically duplicate order outcomes cannot create additional simulated deliveries.
Conflicting terminal outcomes are rejected rather than silently replacing the log.

## Order state and event replay

Order persists one OrderFactRecorded event for each distinct meaningful domain fact.
Consumers of order.events that do not need these records explicitly ignore that type.
Existing OrderCreated/OrderNoteAdded history remains valid; no original events are rewritten.

Order checks each fact's producer aggregate, order/customer identity, event type, schema
and producer version. The Kafka adapter also checks the topic and record key. Inbox identity
and the source payload identity are checked transactionally. Each stream is serialized
against concurrent appends, but facts from different producers are never compared using a
shared "highest version". Out-of-order facts are retained in the event stream.

* COMPLETED requires InventoryReserved, PaymentCompleted and ShipmentCreated.
* InventoryReservationFailed permits cancellation without charging.
* PaymentFailed requires InventoryReserved and InventoryReleased before cancellation.
* ShipmentFailed requires InventoryReserved, PaymentCompleted, PaymentRefunded and
  InventoryReleased before cancellation.
* A payment/shipping failure or refund fact marks the unfinished order COMPENSATING.
  Missing compensation cannot be reported as CANCELLED.

OrderCompleted or OrderCancelled is appended with its matching outbox row after the
prerequisite check. Replaying the stream reconstructs terminal status. Mutually exclusive
producer outcomes are rejected. Duplicate known facts remain no-ops after termination;
unexpected new facts after a terminal outcome require operator investigation.

## Refunds and early compensation

ShipmentFailed is accepted into a durable refund intent, even if the local charge is not
ready. A worker can claim it only after the matching customer's charge is COMPLETED.
Refund claims have expiring tokens, and finalization compares the token so stale workers
cannot overwrite newer work. The original terminal charge remains immutable; refund status
is separate and PaymentRefunded is version 2 of the payment event stream.

The provider refund ledger commits independently and uses the payment UUID as a stable key
in its separate refund namespace. It allows one full refund effect for a successful charge.
A response lost after provider commit leaves the application refund UNKNOWN; recovery
looks up that same effect, then commits REFUNDED and its outbox together. Outbox ordering
keeps PaymentRefunded behind PaymentCompleted, while other topics can still arrive in any
order. The simulator does not support partial refunds or actual funds.

Two additional synthetic postal codes exercise refund recovery when country is `ZZ`:

| Postal code                 | Refund behavior                                                                                              |
|-----------------------------|--------------------------------------------------------------------------------------------------------------|
| REFUND-RETRY                | First two calls are unavailable; third succeeds. Order stays COMPENSATING until refund/release facts arrive. |
| REFUND-TIMEOUT              | Refund commits, then its response is lost; reconciliation finishes without a second refund.                  |
| Any other valid postal code | Refund succeeds immediately.                                                                                 |

Inventory stores PaymentFailed/PaymentRefunded as durable compensation intents. If the
reservation is missing, the intent remains pending. Once the reservation arrives, a short
transaction locks the intent, performs the existing idempotent stock release and commits
its done marker and outbox. Version conflicts roll back the whole attempt and the next
poll retries. Concurrent duplicate compensation cannot restore stock twice. Failed or
already released reservations are not released again. ADMIN release remains a repair/demo
operation; it is not a substitute for provider reconciliation or safe saga compensation.

Consumers retain failures without committing them under the existing retry configuration.
Poison records and contradictory histories can block a partition until corrected; consumer
DLT/redrive is Phase 10. No workflow declares success merely because a retry was exhausted.

## Run and inspect

```bash
python3 infrastructure/scripts/init-env.py
docker compose up -d --build --wait --wait-timeout 300 \
  order-service inventory-service payment-service shipping-service notification-service api-gateway
```

Deployment runs forward migrations and starts consumers on retained events. In particular,
previously PENDING orders can now reach a terminal outcome. The new Shipping and Notification
services use the existing environment-backed ADMIN demo credentials. Their readiness checks
include their own PostgreSQL databases.

Create an order using the [Phase 4 example](phase-4-order-outbox.md) with a stocked product
from [Phase 5](phase-5-inventory.md). For payment decline use `tok_declined`; for shipping
failure use country `ZZ`. For the combined response-loss scenario use `tok_timeout`,
country `ZZ` and postal code `REFUND-TIMEOUT`. Then inspect:

```bash
set -a
source .env
set +a
curl --fail-with-body -u "11111111-1111-1111-1111-111111111111:$DEMO_CUSTOMER_PASSWORD" \
  "http://localhost:8080/api/orders/$ORDER_ID"
curl --fail-with-body -u "admin:$DEMO_ADMIN_PASSWORD" \
  "http://localhost:8080/api/orders/$ORDER_ID/events"
curl --fail-with-body -u "admin:$DEMO_ADMIN_PASSWORD" \
  "http://localhost:8080/api/shipping/$ORDER_ID"
curl --fail-with-body -u "admin:$DEMO_ADMIN_PASSWORD" \
  "http://localhost:8080/api/payments/$ORDER_ID/refund"
curl --fail-with-body -u "admin:$DEMO_ADMIN_PASSWORD" \
  "http://localhost:8080/api/notification/$ORDER_ID"
```

A shipment, refund or notification can return 404 while its prerequisite is pending, or
when that branch does not apply. Orders remain customer-owned; shipping, refund and
notification diagnostics are ADMIN-only. The earlier `verify-orders.py` smoke script now
refreshes versions on concurrent saga appends and accepts the expanded order event history.

## Isolated verification

```bash
./mvnw -pl saga-tests -am verify
./mvnw clean verify
```

The new `saga-tests` module starts the actual packaged order, inventory, payment, shipping,
notification and gateway jars with temporary PostgreSQL databases and a three-partition
Kafka topology. It verifies all five branches in the table above through the gateway and
checks durable charge/refund counts, restored stock and one notification per order. It
also republishes persisted events to verify consumer deduplication. All child applications
and containers are stopped afterward; the existing Compose stack is not used by these tests.

Service tests additionally check early stock compensation, early refund intents, and
order replay across randomized permutations of prerequisite facts. Test prerequisites are
Java 21+, Docker and enough memory for six small application JVMs plus PostgreSQL/Kafka.

On 2026-09-09, Java 21 full-reactor verification passed all 60 tests across 13 modules,
with no failures, errors or skips. All five packaged-service saga scenarios passed, as did
the early-compensation and reordered-fact service tests. Compose configuration and script
syntax/whitespace checks passed. After explicit user approval, all six updated services were
source-built and deployed healthy. Retained synthetic order
`4d124df9-8bee-48ec-97db-bdac907d4fdb` reached CANCELLED at version 4 and produced one
simulated cancellation notification; all four order events were published. Its rejected
inventory correctly produced no payment or shipment. Routed authorization checks passed,
and saga/notification consumers caught up with all retained records. The other saga branches
were verified in the isolated integration suite.
