# Architecture decisions

These decisions constrain the implementation; see README for delivered phase status.

| ADR | Decision and reason | Tradeoff / alternative |
|---|---|---|
| 001 Kafka | Durable partitioned log, independent consumer groups, replay and backpressure support the workflow and projections. | More operational complexity than a queue; no cross-topic total ordering. A simple work queue is preferable without replay/fan-out needs. |
| 002 Saga choreography | Each owner reacts to facts and commits a local step, preserving domain autonomy. | End-to-end flow is harder to inspect and evolve; explicit orchestration can suit complex branching, but is outside the primary workflow. |
| 003 No two-phase commit | Database and broker availability should not be coupled through distributed locks or XA coordinators. | Temporary inconsistency and compensation are application responsibilities. A saga cannot provide global ACID isolation. |
| 004 Polling transactional outbox | Store business changes and publication intent atomically; a poller needs no CDC infrastructure. | Adds write amplification and publication latency. Debezium via Kafka Connect can stream WAL with lower polling overhead, but requires connector operation, replication slots and schema-change planning. Neither eliminates consumer idempotency. |
| 005 Selective event sourcing | Order history has meaningful business transitions and educational reconstruction value. | Upcasters, concurrency and replay complicate development. Catalog/cart remain ordinary relational state; audit logging alone is not event sourcing. |
| 006 CQRS | A dedicated read service joins facts asynchronously and supports cheap customer-order queries. | Lag, duplicate handling and rebuild operations are real costs. Ordinary CRUD is simpler when read/write needs match. |
| 007 PostgreSQL | Transactions, unique constraints, JSONB and versioned relational state support all local consistency requirements. | One development server is a failure domain; independent credentials do not create physical isolation. Production deployment can separate servers. |
| 008 Resilience4j | Circuit breaker, retry, bulkhead and time limiter expose bounded downstream behavior. | Policies can amplify traffic when layered carelessly. Use one bounded retry budget with jitter, explicit HTTP timeouts and cancellation-aware tasks. |
| 009 Composition plus projection | Projection serves the normal order page; details endpoint demonstrates owner-authoritative data and partial availability. | Composition adds latency and dependency fan-out; it is not a consistent cross-service snapshot. |
| 010 Database per service | Data ownership prevents hidden coupling through joins and shared entities. | Duplicated snapshots and asynchronous integration replace convenient foreign keys and joins. |
| 011 Boot 4 dependency discipline | Pin Boot 4.0.8 and Cloud 2025.1.2; use Boot 4 modular starters and the Resilience4j Boot 4 adapter 2.4.0. | Imported BOMs manage framework versions. Compatibility evidence is not a substitute for integration tests. |
| 012 Java 21 and JUnit 5 | Compile with release 21; pin Jupiter 5.14.4 to meet the explicit JUnit 5 requirement instead of inheriting Boot 4's JUnit 6 default. | Spring 7 SpringExtension uses a JUnit 6 Store API and failed with NoSuchMethodError under JUnit 5. Startup tests therefore use explicit SpringApplication lifecycle under JUnit 5. Future API/integration tests must also avoid SpringExtension unless the JUnit requirement changes. |

## Compatibility sources

Verified against official sources on 2026-09-08:

* [Spring Boot 4.0 documentation](https://docs.spring.io/spring-boot/4.0/installing.html) identifies 4.0.8.
* [Spring Cloud compatibility matrix](https://spring.io/projects/spring-cloud/) pairs 2025.1.x with Boot 4.0.x and lists 2025.1.2.
* [Resilience4j 2.4.0 release](https://github.com/resilience4j/resilience4j/releases/tag/v2.4.0) adds Boot 4 / Cloud 5 support.
* [Boot 4 migration guide](https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-4.0-Migration-Guide) documents modular Kafka starters and migration considerations.

The explicit Resilience4j adapter version avoids depending on its omission from the
2.4.0 BOM. Persistence, Kafka, Redis, Flyway, Testcontainers and OpenTelemetry dependencies
will be introduced and resolved in the phases that use them, with Boot-managed versions.
No claim is made that unused libraries have passed runtime integration tests yet.

## ADR 013 — Local infrastructure readiness

Use protocol-level health checks, a PostgreSQL initialization completion marker, and
a successful Kafka topic-init job before starting applications. This makes a fresh
checkout reproducible, while keeping runtime recovery in service code. Named volumes
preserve data; credentials are generated locally and distinct per owner. Single-node
PostgreSQL/Kafka simplify the demo but do not demonstrate high availability.

## ADR 014 — Build from source and provide a trace backend

Multi-stage Dockerfiles build the Java 21 reactor with a shared BuildKit cache, allowing
Compose startup without host-built jars. A small compiled HTTP probe works in distroless
images. This introduces a build-only Go toolchain, not a Go business service. Tempo adds
a real queryable OTLP destination; an exporter configuration alone would not prove trace
delivery. Its local storage and collector memory queue are development tradeoffs.

## ADR 015 — Cart stores quantities, not binding prices

Keep product identity and quantity in cart_db; validate an active product over bounded
HTTP before writing. No remote database access or cross-service FK is introduced.
This avoids stale prices being treated as authoritative but requires checkout to
revalidate. CartTransactions owns short local transactions separately from HTTP calls.
Both client expectedVersion and a JPA versioned database update protect mutations.

## ADR 016 — Protect Phase 3 APIs before the JWT phase

Use environment-backed stateless Basic authentication for local curl demonstrations,
with CUSTOMER resource ownership and ADMIN catalog writes. Gateway forwards credentials;
services enforce authorization. This small service-local adapter is intentionally
replaced by JWT in Phase 11. Shipping unsecured mutation endpoints while waiting for
that phase would leave the ownership behavior untested. No production identity-store
or browser-authentication suitability is claimed.

## ADR 017 — Test actual PostgreSQL schema and HTTP behavior

Use Boot-managed Testcontainers 2.0.5 with explicit lifecycle and JUnit 5. Run Flyway and
Hibernate validation against real PostgreSQL, including a two-writer barrier test.
A controlled HTTP catalog fixture makes transient failure tests deterministic; a
separate gateway/Compose smoke test verifies real interservice communication. The
tradeoff is a Docker requirement for Maven verify; tests do not silently skip it.

## ADR 018 — Durable simulated provider and payment recovery

Accept inventory input into a durable payment intent before contacting the simulator.
Use expiring, token-fenced worker claims and a stable order/payment key for reconciliation
and charges. The simulator ledger commits independently, allowing response-loss and
application-rollback tests to prove that recovery reuses an existing charge. The application
commits terminal payment state and outbox publication intent together.

This models an external provider's durability without real funds or credentials. Both
ledgers share payment_db in the demo; real providers require their own idempotency and
lookup guarantees and bounded network calls. UNKNOWN is never converted to DECLINED merely
because a call timed out. Refund and compensation contracts are deferred to Phase 7.
