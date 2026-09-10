# E-commerce checkout and payment platform

Java 21 / Spring Boot 4 educational microservices project, implemented incrementally in
the requested thirteen phases. **Phases 1–9 are implemented, including event-sourced orders and a complete simulated
checkout saga from accepted order through shipping, compensation and notification.**
Docker Compose provisions isolated databases, Kafka, Redis and observability alongside
all ten applications. Product/cart/order APIs are available through the gateway with local
authentication. CQRS query APIs and generation-based rebuilds are implemented; checkout HTTP orchestration remains
pending.

## Start the Compose stack

```bash
python3 infrastructure/scripts/init-env.py
docker compose up -d --build --wait --wait-timeout 300
python3 infrastructure/scripts/verify.py --apps
python3 infrastructure/scripts/verify-catalog-cart.py
```

See [the infrastructure guide](infrastructure/README.md) for credentials, endpoints,
health checks, infrastructure-only startup and recovery commands.
See [Phase 3: Product and Cart](docs/phase-3-product-cart.md) for executable curl examples,
credentials, versioned mutations, failure experiments and Testcontainers coverage.
See [Phase 4: Order and Outbox](docs/phase-4-order-outbox.md) for order creation, replay,
idempotency, publication and failure recovery.
See [Phase 5: Inventory](docs/phase-5-inventory.md) for stock, reservations, release,
optimistic concurrency and OrderCreated consumption.
See [Phase 6: Payment](docs/phase-6-payment.md) for simulated charges, idempotency,
UNKNOWN outcomes and durable recovery.
See [Phase 7: Choreography](docs/phase-7-saga.md) for complete order outcomes, refunds,
shipping, simulated notifications and isolated end-to-end tests.
See [Phase 8: CQRS projection](docs/phase-8-projection.md) for query APIs and rebuilds.
See [Phase 9: Order details composition](docs/phase-9-composition.md) for owner lookups,
partial availability and customer access controls.

## Current directory structure

```text
ecommerce-platform/                 # this repository (existing folder name retained)
├── pom.xml                         # Boot 4.0.8, Cloud 2025.1.2, Java 21
├── mvnw / mvnw.cmd                  # Maven 3.9.11 wrapper
├── platform-contracts/              # Money, EventEnvelope<T>, unit tests
├── api-gateway/
├── product-service/
├── cart-service/
├── checkout-service/
├── order-service/
├── inventory-service/
├── payment-service/
├── shipping-service/
├── notification-service/
├── order-query-service/
├── saga-tests/                      # packaged-service end-to-end verification
├── docker-compose.yml              # complete infrastructure and service foundations
├── .env.example                    # credential names; generate private .env locally
├── infrastructure/                 # configs, provisioning and smoke tests
└── docs/
    ├── architecture.md             # boundaries, topics, sequences, failure invariants
    ├── decisions.md                # ADRs, tradeoffs, compatibility sources
    └── patterns.md                 # distinctions and interview answers
```

All ten services have executable jars, Dockerfiles, health and metrics configuration.
Product and cart now also contain domain models, JPA repositories, Flyway migrations,
secured REST APIs, DTOs, validation/error handling, unit and PostgreSQL integration tests.
Order includes an append-only JDBC event store, atomic command idempotency/outbox writes,
reconstructed reads, secured APIs and ordered Kafka publication with integration tests.
Inventory now adds versioned stock, transactional reservations/inbox/outbox, an OrderCreated
consumer, administrative APIs and PostgreSQL/Kafka tests.
Payment adds durable intents, an InventoryReserved consumer, a persisted provider simulator,
lease-based recovery and terminal payment events through its outbox.
Shipping, notification, refund/stock compensation and order status consumers now complete
the saga; saga-tests verifies the actual packaged applications together.
Checkout orchestration gains its business implementation in a later phase.

## Build and test

Use JDK 21+ and a running Docker engine for Testcontainers. The wrapper downloads Maven
on its first invocation. Integration tests create isolated databases and do not use the
running Compose business databases.

```bash
./mvnw clean verify
./mvnw -pl product-service,cart-service -am verify
```

Observe the running stack:

```bash
curl --fail http://localhost:8080/actuator/health
curl --fail http://localhost:9090/-/ready
python3 infrastructure/scripts/verify-catalog-cart.py --outage
```

Business APIs require demo credentials from `.env`; see the phase guides for access rules.
The gateway routes product, cart, order, inventory, payment, shipping, notification and
order-view APIs, including composed order details. Database-backed business services
include PostgreSQL in readiness. JWT authentication and checkout HTTP orchestration
remain pending. Phase 9 source changes must be deployed before using its new endpoints
on an existing Phase 8 stack.

Build/start a single service from source:

```bash
docker compose up -d --build --wait product-service
```

For IDE or host-jar development with the infrastructure running:

```bash
set -a
source .env
set +a
DB_PASSWORD="$PRODUCT_DB_PASSWORD" java -jar product-service/target/product-service-1.0.0-SNAPSHOT.jar --spring.profiles.active=demo
```

Compose uses internal service ports, leaving host 8081 available for the host product
process. A host cart process uses `DB_PASSWORD="$CART_DB_PASSWORD"` and the default
catalog address `http://localhost:8081`. Do not expose these local Basic-auth APIs publicly.

## Architecture and learning guide

[Architecture](docs/architecture.md) includes the ownership table, communication map,
success and compensation sequence diagrams, Kafka contracts and concurrency rules.
[Decisions](docs/decisions.md) records the ADRs and library compatibility evidence.
[Patterns](docs/patterns.md) maps each pattern to its purpose, limits and interview answer.

## Implementation progress

| Phase | Deliverable                                                                              | Status      |
|-------|------------------------------------------------------------------------------------------|-------------|
| 1     | Architecture, module foundation, generic contracts                                       | Implemented |
| 2     | Compose, isolated PostgreSQL databases, Kafka KRaft, Redis, observability infrastructure | Implemented |
| 3     | Product and cart APIs, migrations, seed data                                             | Implemented |
| 4     | Event-sourced order, outbox and reconstruction                                           | Implemented |
| 5     | Inventory reservations and optimistic concurrency                                        | Implemented |
| 6     | Payment simulator, persisted idempotency and recovery                                    | Implemented |
| 7     | Choreography, shipping, notifications and compensation                                   | Implemented |
| 8     | CQRS projection and rebuild                                                              | Implemented |
| 9     | Order details composition                                                                | Implemented |
| 10    | Resilience, retries, DLT and failure controls                                            | Next        |
| 11    | JWT and end-to-end tracing / business metrics                                            | Pending     |
| 12    | Integration, API, messaging, concurrency and saga tests                                  | Pending     |
| 13    | Executable full walkthrough, operational recovery and final documentation                | Pending     |

End-to-end curl examples will be added with working APIs so the walkthrough remains
executable. The final target is `docker compose up -d --build` plus `./mvnw clean verify`.

## Phase 1 verification record

On 2026-09-08, `mvn clean verify` on the installed Java 21 runtime passed all 12
reactor projects: 15 tests, zero failures/errors/skips. The packaged product-service
jar was started on a temporary loopback port; health and readiness returned UP and
Prometheus exported JVM memory metrics. The process was stopped after verification.
Docker was stopped during Phase 1; Phase 2 starts and verifies the Compose infrastructure.

Spring Framework 7's JUnit extension required a JUnit 6 method in the initial startup
test. To retain the requested JUnit 5, tests explicitly manage SpringApplication lifecycle
instead of using SpringExtension. See ADR 012. Maven is pinned by the generated wrapper;
the verification above used installed Maven 3.9.16, not the wrapper distribution.

## Phase 2 verification record

On 2026-09-08, the complete source-built Compose deployment passed readiness checks:
18 long-running containers healthy, plus the successfully completed Kafka initializer.
The repeatable `verify.py --apps` check passed all nine database ownership/isolation
checks, Redis authentication, Kafka topic configuration and message delivery, native
Prometheus/collector configuration validation, synthetic OTLP trace retrieval from Tempo,
and all ten application Prometheus scrape targets. Images were built and run on the
local ARM64 Docker runtime. Other architectures have not been exercised in this session.

The stack is left running for inspection. Use `docker compose stop` to pause it without
deleting data. No checkout or saga correctness is claimed by these infrastructure checks.

## Phase 3 verification record

On 2026-09-08, the full Java 21 Maven reactor passed. Focused verification after adding
strict integer deserialization, catalog inactivity and timeout scenarios also passed.
Current Surefire/Failsafe reports contain 24 tests, zero failures/errors/skips, including
six PostgreSQL-backed API/concurrency integration tests.

The source-built gateway/product/cart deployment passed readiness against the existing
Compose databases, including first-run Flyway and Hibernate schema validation. The
real routed smoke test passed with an actual product-service outage and recovery:
failed catalog access returned 503 without changing cart version or quantities.
Product-service was restored; no seed products were changed. The stack remains running.

## Phase 4 verification record

On 2026-09-09, `mvn clean verify` with Java 21 passed all 12 reactor modules:
33 tests, zero failures/errors/skips. Order contributes four domain/contract tests and
six PostgreSQL/Kafka integration tests, including concurrent commands/pollers, rollback,
ordered publication and stable duplicate identity after an acknowledgement/rollback.

The source-built Compose order-service and gateway are running healthy. Read-only routed
checks returned health 200, unauthenticated order access 401 and an authenticated missing
order 404. After user approval, `python3 infrastructure/scripts/verify-orders.py` passed
against the running gateway: authorization, idempotent creation, reconstruction, version
conflicts and Kafka acknowledgement. It retained synthetic order
`4d124df9-8bee-48ec-97db-bdac907d4fdb` with two immutable events and published outbox rows.
Broker publication was also verified in isolated Testcontainers.

## Phase 5 verification record

On 2026-09-09, `mvn clean verify` with Java 21 passed all 12 modules:
41 tests, zero failures/errors/skips. Inventory contributes two unit tests and seven
PostgreSQL/Kafka integration tests covering stock constraints, reservation/release races,
rollback, event identity/schema validation and real Kafka consumption/publication.
`docker compose config --quiet` and `git diff --check` passed.

After explicit user approval, the source-built inventory service and gateway were deployed
and are running healthy. Routed checks verified 401/403 access controls and seeded stock.
Inventory consumed retained synthetic order `4d124df9-8bee-48ec-97db-bdac907d4fdb`,
correctly rejected its unstocked product with STOCK_NOT_FOUND, and published
InventoryReservationFailed with Kafka acknowledgement. Successful reservations, release and
concurrency behavior were verified in isolated PostgreSQL/Kafka containers.

## Phase 6 verification record

On 2026-09-09, Java 21 `mvn clean verify` passed all 12 modules:
52 tests, zero failures/errors/skips. Payment contributes two unit tests and ten
PostgreSQL/Kafka integration tests. Verification includes successful/declined charges,
response loss, transient provider failures, concurrency, changed instructions, expired-lease
fencing, provider-commit/application-rollback recovery, automatic recovery after an
application restart, API ownership and actual Kafka duplicate publication after rollback.
Compose configuration and whitespace checks passed.

After explicit user approval, the source-built payment service and gateway were deployed
and are running healthy. Routed checks verified health 200, unauthenticated payment access
401, customer access to provider diagnostics 403 and missing payment 404. The payment
consumer caught up with the retained inventory event (offset 1 of 1, lag 0; the other two
partitions were empty). The inventory-rejected synthetic order correctly has no payment.
Successful charges, declines and recovery were verified in isolated PostgreSQL/Kafka tests;
no new shared-stack charge records were created by the deployment checks.

## Phase 7 verification record

On 2026-09-09, Java 21 `mvn clean verify` passed all 13 modules: 60 tests,
zero failures/errors/skips. The five packaged-service saga tests exercised successful
completion, inventory rejection, payment decline with stock release, shipment failure with
retried refund, and lost responses during both charge and refund. They also checked event
redelivery, one provider effect, one notification, gateway authorization and restored stock.
Service tests verified early compensation/refund retention and order replay under reordered
facts. Compose configuration, smoke-script syntax and whitespace checks passed.

After explicit user approval, all six updated services were source-built and deployed;
order, inventory, payment, shipping, notification and gateway are healthy. The retained
synthetic order `4d124df9-8bee-48ec-97db-bdac907d4fdb` reached CANCELLED at version 4,
with one simulated cancellation notification and all four order events published. Its
inventory rejection correctly produced no payment or shipment. Routed authorization checks
passed. Order-saga and notification consumers reached zero lag on their nonempty partitions;
the remaining partitions were empty. Other saga branches passed in isolated integration tests.

## Phase 8 verification record

See [CQRS projection and rebuild](docs/phase-8-projection.md) for query APIs,
replay guarantees and recovery limitations. Java 21 `mvn verify` passed across all 13 modules: 68 tests, zero failures,
errors or skips. Compose configuration and whitespace checks passed. The query service and gateway were rebuilt and
deployed healthy. A read-only gateway check verified the retained synthetic order as CANCELLED at version 4, inventory
REJECTED and notified. The journal contained six events, one visible order and zero buffered events. Rebuild execution
was verified in isolated integration tests.

## Phase 9 verification record

See [Order details composition](docs/phase-9-composition.md) for availability semantics,
owner access controls and bounded parallel lookups. Java 21 affected-reactor verification
passed, including packaged-service gateway checks. Together with the preceding run of
unaffected modules, current reports contain 73 tests, zero failures, errors or skips.
Compose configuration and whitespace checks passed. Phase 9 is not yet deployed to the
shared Compose stack.
