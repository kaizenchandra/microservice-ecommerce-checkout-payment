# Phase 2 — runnable infrastructure

Compose starts PostgreSQL, Kafka KRaft, topic initialization, Kafka UI, Redis, Prometheus,
Grafana, Tempo, an OpenTelemetry collector, and the ten Phase 1 application foundations.
Phase 3 adds secured product/cart APIs and gateway routes; saga consumers arrive later.
See [the Phase 3 guide](../docs/phase-3-product-cart.md) for the business walkthrough.

## Start from a clean checkout

Requirements: Docker Engine with Compose v2+ (or Docker Desktop/OrbStack), Python 3 for
the environment helper and smoke tests, and internet access for the first image build.
Allow roughly 8 GB RAM and several GB of image/build storage for this educational stack.
A host JDK/Maven installation is not required for container builds.

```bash
python3 infrastructure/scripts/init-env.py
bash infrastructure/scripts/validate.sh
docker compose up -d --build --wait --wait-timeout 300
python3 infrastructure/scripts/verify.py --apps
```

The initializer creates an ignored `.env` with random independent credentials and mode

0600. Existing values in `.env` are never overwritten; missing template keys are appended. Compose refuses to start with
      missing/empty
      passwords. `.env.example` documents names without containing working credentials.
      Use your local `.env` to find the Grafana admin password; no token issuer exists yet.

All image versions are pinned in Compose/Dockerfiles. Each application uses a multi-stage
Maven/Java 21 build. BuildKit shares the identical reactor build layer across the ten
images and caches Maven downloads. Final application images run as UID 10001 and contain
only the JRE, executable jar and a small static HTTP health probe. The probe also works
in the collector's distroless image without requiring a shell.

For infrastructure only (e.g. run application jars in the IDE):

```bash
docker compose up -d --build --wait --wait-timeout 300 \
  postgres kafka kafka-init redis prometheus tempo otel-collector grafana kafka-ui
python3 infrastructure/scripts/verify.py
```

Application scrape targets will be DOWN in this mode until their containers start;
Prometheus uses Compose DNS names, so IDE-hosted apps require a deliberate scrape config
change. Missing applications are not an infrastructure startup failure.

## Directory structure

```text
infrastructure/
├── postgres/01-databases.sh           # distinct databases, logins and privileges
├── kafka/topics.sh                   # idempotent topic provisioning
├── prometheus/prometheus.yml         # all ten application targets
├── prometheus/alerts.yml              # unavailable application alert
├── grafana/provisioning/              # Prometheus / Tempo and dashboard provisioning
├── grafana/dashboards/platform.json   # availability, heap and HTTP rates
├── otel/collector.yml                 # bounded tracing pipeline
├── tempo/tempo.yml                    # local trace store, 24-hour retention
├── healthcheck/                       # static Go HTTP probe and image stages
└── scripts/
    ├── init-env.py                    # local credential generation
    ├── validate.sh                    # configuration syntax checks
    └── verify.py                      # real infrastructure smoke tests
```

## Network and ownership

| Component            | Host endpoint (loopback only)         | Container endpoint          |
|----------------------|---------------------------------------|-----------------------------|
| Gateway foundation   | http://localhost:8080/actuator/health | api-gateway:8080            |
| PostgreSQL           | localhost:5432                        | postgres:5432               |
| Kafka                | localhost:9092                        | kafka:19092                 |
| Redis                | localhost:6379                        | redis:6379                  |
| Kafka UI (read-only) | http://localhost:8090                 | kafka-ui:8080               |
| Prometheus           | http://localhost:9090                 | prometheus:9090             |
| Grafana              | http://localhost:3000                 | grafana:3000                |
| OTLP gRPC / HTTP     | localhost:4317 / localhost:4318       | otel-collector:4317 / :4318 |
| Tempo                | internal only                         | tempo:3200                  |

Port overrides are documented in `.env.example`. Other application ports are internal;
inspect their health with Compose exec or Prometheus. The gateway routes product/cart APIs from Phase 3. Infrastructure
protocols are local development plaintext; loopback
bindings do not constitute production TLS or Kafka ACLs.

Nine databases use `<domain>_db` and `<domain>_owner` for product, cart, checkout, order,
inventory, payment, shipping, notification and query. Each owner can create its own
migration tables and has no CONNECT permission to another service database or postgres.
The bootstrap administrator alone can administer all databases. The gateway has no DB
credentials. Application containers never receive bootstrap or other owners' passwords.
Roles and databases are deployment bootstrap objects; service-local Flyway migrations
will create business tables as each service is implemented. No shared business schema.

Initialization runs only for an empty PostgreSQL volume. Its readiness marker is written
after all grants succeed; `pg_isready` alone could expose a partially initialized server.
If bootstrap fails, inspect the logs and repair deliberately; do not run production
schema DDL through this initialization script on every startup.

## Kafka communication and durability

The broker combines controller/broker roles using KRaft and persists data in a named
volume. There is no ZooKeeper. Clients inside Compose use `kafka:19092`; host clients
use `localhost:9092`. Advertised listeners match those two network contexts.

`kafka-init` waits for broker API readiness and creates five domain topics plus
`infrastructure.smoke`, each with three partitions and seven-day retention. It is safe
to repeat: `--if-not-exists` leaves existing topics intact. Changing the script does not
alter settings of existing topics; use an explicit reviewed topic configuration change.
Automatic topic creation is disabled to catch naming mistakes.

Replication factor and minimum ISR are one for this single-broker demo. `acks=all`
therefore acknowledges one replica, not redundant durability. Kafka ordering applies
within a topic partition; equal order keys across topics do not establish total order.
Consumer-specific DLT topics are provisioned by the initializer and application KafkaAdmin.
See [Phase 10 recovery](../docs/phase-10-resilience.md) for bounded listener retries,
retention, acknowledgment guarantees and the dry-run-first redrive command.

## Readiness and failure behavior

Compose health conditions gate bootstrap dependencies; application startup waits for
PostgreSQL initialization, Kafka topics and authenticated Redis readiness. A failed
one-shot initializer prevents dependent applications from starting. Containers use
bounded logs and persistent volumes. Runtime services restart unless explicitly stopped.
Health becoming unhealthy does not itself cause a Docker restart.

Product/cart readiness now includes their PostgreSQL connection; both run Flyway and
JPA validation at startup. Other application readiness still checks the Spring foundation.
Kafka workflow consumers are not active yet. Infrastructure health must not be mistaken
for a working checkout. Future clients still need reconnect/retry logic after dependency failure;
Compose startup ordering is not runtime resilience.

Redis is configured with a 128 MB bound, LRU eviction and AOF. It is disposable cache
infrastructure. Payment/idempotency and inventory truth must remain in PostgreSQL.

The collector has a memory limiter, bounded batch queue and bounded retry to Tempo.
Its queue is in memory and can lose spans on restart or after the retry budget; this
is telemetry infrastructure, never a reliable business event transport. Tempo persists
traces in its own volume. Explicit Java HTTP/Kafka instrumentation is implemented
in [Phase 11](../docs/phase-11-security-observability.md).

## Observe and verify

```bash
curl --fail http://localhost:8080/actuator/health
curl --fail http://localhost:9090/-/ready
curl --fail http://localhost:3000/api/health
docker compose ps -a
docker compose logs --tail=50 postgres kafka otel-collector
python3 infrastructure/scripts/verify.py --apps
```

The smoke test verifies:

1. All nine owners can perform an ACID transaction containing DDL and data in their
   own database; changes are rolled back. A connection to another owner's database
   must fail specifically with a permission error.
2. Authenticated Redis PING, correct Kafka partition/replica counts, and acknowledged
   publication followed by consumption of a uniquely identifiable test record.
3. Kafka UI, Grafana and Prometheus health, plus native Prometheus and collector config
   validation.
4. A synthetic OTLP span sent to the collector becomes queryable in Tempo.
5. With `--apps`, all ten application targets report UP in Prometheus.

The only retained test data is Kafka smoke records and short-lived synthetic traces.
No business tables or sample customer data are created. The Grafana `Checkout Platform`
dashboard displays actual JVM/HTTP metrics; business panels include committed outcomes, listener attempts and DLT
recovery.
Prometheus evaluates `ApplicationUnavailable` after two minutes; no Alertmanager or
external message delivery is configured.

## Stop, restart and recover

```bash
docker compose stop                 # keep containers and data
docker compose up -d --wait         # resume existing stack
docker compose down                # remove containers/network, keep named volumes
```

Do not delete volumes to handle ordinary failures. `docker compose down -v` is a **destructive reset** of databases,
Kafka events, dashboards and telemetry; use it only
when intentionally discarding this demo's data. Changing `.env` passwords does not rotate
persisted database roles or Grafana accounts. Coordinate credential rotation with those
systems; do not regenerate `.env` against existing data and expect it to update logins.

For a reversible outage experiment, stop/start Kafka and inspect health/UI changes:

```bash
docker compose stop kafka
docker compose start kafka
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list
```

Run the smoke test again after broker recovery. Application outbox retries and saga
compensation cannot be observed until the corresponding phases are implemented.

## Interview question

**Why is `depends_on` insufficient for microservice resilience?** It controls startup
conditions, not the lifetime availability of a dependency. Kafka can fail after an
application starts. Connection recovery, bounded retry, transactional outbox persistence
and idempotent consumption remain application responsibilities. Health probes expose
state but do not implement recovery of business operations.

## Primary references

* [Apache Kafka Docker guide](https://kafka.apache.org/40/getting-started/docker/) for the official KRaft image.
* [Kafbat UI](https://github.com/kafbat/kafka-ui) for the maintained Kafka management UI.
* [Tempo local deployment](https://grafana.com/docs/tempo/latest/set-up-for-tracing/setup-tempo/deploy/locally/) for a
  monolithic development trace store.

Version compatibility is validated with the pinned images in this project; these are
a reproducible demo selection, not a promise that the pins are the newest releases.

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
