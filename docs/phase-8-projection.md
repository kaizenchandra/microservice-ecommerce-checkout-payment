# Phase 8 — CQRS projection and rebuild

The order-query service consumes order, inventory, payment, shipping and notification
streams into its own PostgreSQL database. Customer reads never contact the write services.
The gateway exposes these APIs using the existing demo Basic credentials:

| Method and path | Access | Result |
|---|---|---|
| GET `/api/order-views/{orderId}` | Owner or admin | Order snapshot, notes, outcome and service statuses |
| GET `/api/order-views?page=0&size=20&status=COMPLETED` | Customer or admin | Owned orders (all for admin), newest first |
| GET `/api/order-views/admin/projection` | Admin | Active/building generations, journal and buffered counts |
| POST `/api/order-views/admin/rebuild` | Admin | 202 with generation; 409 if already building |

An inaccessible order returns 404. Pages are limited to 100 entries. The order stream
is authoritative for the overall status; independent service statuses are eventually
consistent. `sourceVersions` exposes the contiguous version applied for each producer.
Missing predecessors remain buffered, including events received before OrderCreated.

## Durable input and replay

Each accepted event is validated against its topic, key, producer, schema and payload.
A local immutable journal stores only fields required by the projection: payment tokens
and unrelated checkout fields are discarded. Addresses and notes remain personal data
in this journal and require appropriate access and retention controls in production.
Duplicate event IDs or equivalent producer versions have no repeated effect; conflicting
content is rejected. Journal, inbox and active view changes commit atomically.

Rebuilds fold the persisted journal into a separate generation in bounded batches.
Reads continue using the active generation. Event ingestion and the final catch-up/switch
hold the same control-row lock, preventing lost events at cutover. The cursor survives
restart. A failed rebuild preserves the active generation and records a sanitized error;
an admin can start a fresh rebuild. Retired and failed generations are retained.

This demo serializes ingestion and rebuild batches and refolds each affected order's
history. It prioritizes understandable correctness over high throughput. Rebuilds can
only recover events already ingested into the local journal. Events lost before ingestion
or expired from Kafka require an external archive/import capability, which is not
implemented. Remote order-details composition is Phase 9.

## Verification

The focused PostgreSQL/Kafka integration suite covers gaps, redelivery, conflicts,
ownership, rollback, token removal, live ingestion during rebuild, failed builds and
restart recovery. The packaged-service saga suite additionally checks successful and
refunded orders and rebuild equality through the gateway against real producer events.

Java 21 `mvn verify` passed across all 13 modules: 68 tests, zero failures, errors or skips. Compose configuration and whitespace checks passed. The query service and gateway were rebuilt and deployed healthy. A read-only gateway check verified the retained synthetic order as CANCELLED at version 4, inventory REJECTED and notified. The journal contained six events, one visible order and zero buffered events. Rebuild execution was verified in isolated integration tests.
