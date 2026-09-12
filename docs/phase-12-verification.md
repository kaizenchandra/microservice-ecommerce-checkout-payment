# Phase 12 — Integrated verification

The test suite combines small domain/contract tests, HTTP and database integration
tests, real Kafka tests and packaged-service scenarios. The packaged suite now starts
nine application jars: gateway, product, cart, order, inventory, payment, shipping,
notification and query. Each stateful service uses its own database in a temporary
PostgreSQL container; messaging uses a temporary three-partition Kafka broker.

JWT is required throughout the packaged suite. It exercises actual HTTP clients,
service security, migrations, outbox publishers and consumers. Service-level tests
retain explicit Basic-auth compatibility where needed to focus their existing API
checks. An in-process OTLP receiver verifies actual span export without using the
shared collector or Tempo.

## Coverage map

| Boundary                   | Evidence                                                                                                                                     |
|----------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| Domain and event contracts | Money validation, order transitions, schema compatibility, event identity and metadata                                                       |
| Product and cart           | Ownership, price/quantity validation, optimistic version conflicts, concurrent updates and catalog failures                                  |
| Order/event store          | Idempotent commands, duplicate and changed requests, concurrent append/version handling, transaction rollback and ordered outbox publication |
| Inventory                  | Last-unit races with a database barrier, duplicate reservations, release, rollback, stock invariants and real Kafka consumption              |
| Payment/refund             | Declines, response loss, stable provider effects, lease fencing, rollback, retries and restart recovery                                      |
| Projection                 | Gaps and reordered facts, deduplication, ownership, generations, concurrent ingestion/rebuild, failed rebuild and restart recovery           |
| Resilience                 | Circuit opening/probes, dependency isolation, timeout/concurrency bounds, DLT acknowledgment failures and real redrive                       |
| Security/observability     | JWT signatures/claims/roles, gateway and owner checks, OTLP parent relationships, MDC context and bounded metrics                            |
| Packaged workflows         | Completion, rejection, decline, shipment failure/refund, duplicate events, notifications and gateway projection rebuild                      |

## Additional packaged scenarios

* **Catalog/cart outage:** create a product as admin and a cart as its customer using
  JWT through the gateway. Verify unauthorized and cross-customer rejection. Stop the
  actual isolated product process, attempt a cart mutation, and assert 503 with exactly
  the previous cart state. Restart on the same port/database, verify the mutation works,
  and check that the stale version is rejected without another change.
* **Concurrent command retries:** release six HTTP callers together with the same order
  command and idempotency key. All receive the same accepted response. Assert one command
  record, one OrderCreated event, one provider charge and one notification. Reusing the
  key with changed instructions returns 409.
* **Competing orders:** submit two orders together for one stocked unit. Their order keys
  target different Kafka partitions, and the isolated inventory consumer runs with
  concurrency three. Assert one completed order, one cancelled order, one charge, one
  reservation, zero remaining availability, and the corresponding projections and
  notifications. The inventory service's separate barrier test specifically exercises
  overlapping database transactions.

Test fixtures generate fresh IDs. No shared-stack orders, stock, charges or notifications
are created. The catalog outage targets only the process launched by the test. Processes
are stopped after the suite; failed process startup reports the captured service log.
Assertions use bounded waits for observable state. Concurrency tests coordinate their
start with latches rather than assuming two sequential calls overlap.

## Run and inspect

Use Java 21, Maven 3.9+ and a running Docker-compatible engine:

```bash
mvn verify
bash infrastructure/scripts/validate.sh
git diff --check
```

`mvn -o verify` also works when all pinned dependencies and required container images
are already cached. Maven's offline switch controls dependency resolution, not Docker
image pulls. Docker unavailability fails the integration tests; they are not silently
skipped. Test reports are under each module's `target/surefire-reports` and
`target/failsafe-reports`. The saga suite's temporary directory contains per-process
logs and redrive output when a failure needs investigation.

A narrower run that still builds the packaged services is:

```bash
mvn -pl saga-tests -am verify
```

## Limits and verification record

This suite verifies the implemented order-acceptance-to-fulfillment workflow. Checkout
HTTP orchestration remains absent, so it does not claim a cart-to-checkout purchase flow.
It does not establish production load capacity, multi-broker durability or a production
identity provider. Shared Compose deployment is a separate operation.

Java 21 `mvn -o verify` passed all 13 reactor modules: 89 tests with zero failures,
errors or skips, including all 12 packaged-service scenarios. Compose configuration,
shell/Python syntax, dashboard JSON and whitespace checks passed. The new tests passed
against the actual packaged services, including stopping and restarting the isolated
catalog process. No application implementation changes were required in this phase.
