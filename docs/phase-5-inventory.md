# Phase 5 — Inventory reservations and optimistic concurrency

Inventory owns stock and reservations in `inventory_db`. It consumes `OrderCreated`
from `order.events` and emits `InventoryReserved` or `InventoryReservationFailed` through
its transactional outbox. It also supports idempotent administrative release, emitting
`InventoryReleased`. Payment, automatic compensation and order status consumers arrive
in later phases. Order remains PENDING at this stage, even after inventory responds.

## Stock and reservation semantics

`onHand` includes reserved units. `available = onHand - reserved`; reserving increases
`reserved`, and releasing decreases it. Neither operation changes `onHand`. Shipment
consumption is not implemented in this phase. Stock is bounded between zero and one
million units per product. Administrators cannot reduce on-hand stock below reserved
units. Missing product stock causes a durable rejection, never an invented quantity.
Inventory stores product UUIDs without cross-database joins or foreign keys to Product.

Each stock entity has JPA `@Version`. Reservation attempts load lines in deterministic
product order, check every line, then mutate and flush all stock within one transaction.
An optimistic conflict rolls back every stock update, the inbox marker, reservation and
outgoing event. The application retries through a separate transactional bean, up to
four attempts with bounded jitter. Each retry loads fresh stock. Exhausted contention
propagates to Kafka retry; it is never turned into an inventory rejection.

Insufficient or missing stock is a business outcome, recorded as REJECTED with a failure
event. A reservation has one stable order ID. Redelivering the event, including with a
new event ID, cannot reserve twice. A different reservation request for that order ID
conflicts. Rejection is terminal for that order: replenishing stock does not resurrect an
old rejected request. A later purchase needs a new order ID.

Release locks the reservation row and changes RESERVED to RELEASED exactly once, even
with concurrent callers or different command IDs. Stock updates still use optimistic
concurrency against other orders. Releasing a rejected or already released reservation
is a no-op. Missing reservations return 404 without recording an inbox marker. Phase 7
must retain/retry compensation facts that arrive before their reservation; no payment
consumer or early-release buffer is claimed here. ADMIN release is a demo/repair action,
not an automatic payment compensation decision.

## Messaging and transactions

`processed_event` deduplicates event identity, and a canonical consumer-owned payload
hash detects changed content. `reservation` additionally deduplicates order identity.
These writes, stock mutations and `outbox_event` commit in one PostgreSQL transaction.
The order producer's complete snapshot is mapped into local records; Inventory has no
Java dependency on order-service. Additive producer fields are ignored. Order identity,
Kafka key, producer aggregate, version, schema, quantities and synthetic payment inputs
are validated before changing stock.

Inventory envelope aggregate type is `InventoryReservation`; aggregate ID and Kafka key
are the order UUID. Reservation outcomes start at version 1; release is version 2.
Correlation and causation link the outgoing event to the incoming OrderCreated, with
traceparent forwarded. These versions belong to Inventory, independent of Order versions.

| Event | Payload |
|---|---|
| InventoryReserved | orderId, customerId, product/quantity items, total, fake paymentToken, synthetic shippingAddress |
| InventoryReservationFailed | orderId, customerId, items, reason (`STOCK_NOT_FOUND` or `INSUFFICIENT_STOCK`) |
| InventoryReleased | orderId, customerId, items, reason (`ADMIN_RELEASE` for the current API) |

The reserved event carries the trusted order total and fake token for the future payment
consumer and synthetic address for downstream shipment events. Inventory does not reprice
orders. Stock/reservation REST responses omit payment inputs and addresses. As in Phase 4,
these shared educational topics are only for synthetic data; real payment data needs a
separate restricted contract and customer data needs a retention policy.

The poller follows Phase 4's oldest-pending-event-per-stream locking rule and waits for
Kafka acknowledgement before marking publication. Send failure backs off without allowing
a release event to overtake its reservation. A crash after acknowledgement but before
commit can duplicate the same event ID. Consumers must deduplicate transactionally.
Outbox identity and payload are protected against updates by a database trigger.

Consumer group is `inventory-reservations-v1`, auto-commit is disabled, and offsets are
acknowledged after each successful listener call. Known unrelated OrderNoteAdded,
OrderCompleted and OrderCancelled events are ignored. Unknown types or invalid schemas
remain failures. Until Phase 10 adds DLT/redrive, failed records retry with a one-second
backoff without a successful recovery/offset commit. A poison event can block its partition;
correct the reader or operator-controlled input before resuming. Startup uses earliest
retained offsets for a new group, so starting against an existing broker processes its
retained OrderCreated events. Kafka retention is not a permanent event archive.

Metrics include `inventory.reservations.accepted`, `inventory.reservations.rejected`,
`inventory.reservations.released`, and the service-tagged `outbox.*` metrics from Phase 4.

## Run and inspect

```bash
python3 infrastructure/scripts/init-env.py
docker compose up -d --build --wait --wait-timeout 300 inventory-service api-gateway
set -a
source .env
set +a
PRODUCT_ID=aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1
curl --fail-with-body -u "admin:$DEMO_ADMIN_PASSWORD" \
  "http://localhost:8080/api/inventory/stock/$PRODUCT_ID"
```

Compose enables the demo Flyway seed: laptop 10, keyboard 50, monitor 20, headphones 30.
The seed runs once, so application restarts do not replenish consumed availability.
The default host profile has schema migrations only; add `--spring.profiles.active=demo`
for these seed rows. Inventory readiness includes its database.

All `/api/inventory/**` endpoints require ADMIN under the existing local Basic-auth
adapter. Customers receive 403; JWT and service identity are introduced in Phase 11.

| Method and path | Purpose |
|---|---|
| POST /api/inventory/stock | Create stock with `{productId, onHand}`; 201 or duplicate 409 |
| GET /api/inventory/stock/{productId} | Read onHand, reserved, available and version |
| PUT /api/inventory/stock/{productId} | Set `{onHand, expectedVersion}`; stale version 409 |
| GET /api/inventory/reservations/{orderId} | Inspect durable RESERVED, REJECTED or RELEASED state |
| POST /api/inventory/reservations/{orderId}/release | Release with UUID `Idempotency-Key` header |
| GET /api/inventory/reservations/{orderId}/outbox | Inspect outgoing events and publication status |

Use the [Phase 4 order creation example](phase-4-order-outbox.md) with one of the seeded
product UUIDs, then poll the reservation endpoint with its ORDER_ID. It may initially
return 404 while the order outbox or consumer catches up. To release that synthetic order:

```bash
curl --fail-with-body -u "admin:$DEMO_ADMIN_PASSWORD" \
  -X POST -H "Idempotency-Key: $(uuidgen)" \
  "http://localhost:8080/api/inventory/reservations/$ORDER_ID/release"
```

For an isolated last-unit demonstration, the integration suite creates its own products
and orders, starts two concurrent transactions and checks that exactly one reserves.
It does not modify the existing Compose databases or broker.

## Verification

```bash
./mvnw -pl inventory-service -am verify
./mvnw clean verify
```

Tests start isolated PostgreSQL 17 and Kafka containers with real Flyway, JPA, REST security
and Kafka listener/publisher configuration. They cover stock validation, seed data,
versioned administration, atomic multi-line rejection, missing stock, last-unit contention,
concurrent duplicate orders and release, forced JPA version conflicts, injected outbox
failure/rollback, event schema/identity validation, actual Kafka consumption/publication,
metadata propagation and the acknowledgement-before-commit duplicate window.

On 2026-09-09, Java 21 `mvn clean verify` passed all 12 modules with 41 tests and no
failures, errors or skips. Inventory's two unit tests and seven integration tests passed.
Compose configuration validation and whitespace checks passed. After explicit user approval,
the source-built inventory service and gateway were deployed and reported healthy. Routed
checks verified unauthenticated 401, customer 403 and ADMIN access to seeded stock. The
retained Phase 4 synthetic order `4d124df9-8bee-48ec-97db-bdac907d4fdb` was consumed and
correctly rejected with STOCK_NOT_FOUND because its random product UUID has no stock row.
Its InventoryReservationFailed outbox event was acknowledged by Kafka and marked PUBLISHED.
Successful reservations, release and contention scenarios passed in isolated containers.
