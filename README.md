# E-commerce checkout and payment platform

Java 21 / Spring Boot 4 educational microservices project, implemented incrementally in
the requested thirteen phases. **Phases 1–3 are implemented: architecture, runnable infrastructure, and product/cart APIs.**
Docker Compose provisions isolated databases, Kafka, Redis and observability alongside
all ten applications. Product/cart APIs are available through the gateway with local
authentication. Checkout APIs and saga consumers remain pending.

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
The remaining service foundations gain their business implementation in later phases.

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

Product/cart business APIs require demo credentials from `.env`; see the Phase 3 guide.
Their readiness includes PostgreSQL. Other applications still expose foundation health.
The gateway routes product/cart APIs; JWT and other service routes arrive later.

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

| Phase | Deliverable | Status |
|---|---|---|
| 1 | Architecture, module foundation, generic contracts | Implemented |
| 2 | Compose, isolated PostgreSQL databases, Kafka KRaft, Redis, observability infrastructure | Implemented |
| 3 | Product and cart APIs, migrations, seed data | Implemented |
| 4 | Event-sourced order, outbox and reconstruction | Next |
| 5 | Inventory reservations and optimistic concurrency | Pending |
| 6 | Payment simulator, persisted idempotency and recovery | Pending |
| 7 | Choreography, shipping, notifications and compensation | Pending |
| 8 | CQRS projection and rebuild | Pending |
| 9 | Order details composition | Pending |
| 10 | Resilience, retries, DLT and failure controls | Pending |
| 11 | JWT and end-to-end tracing / business metrics | Pending |
| 12 | Integration, API, messaging, concurrency and saga tests | Pending |
| 13 | Executable full walkthrough, operational recovery and final documentation | Pending |

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
