# Patterns and interview notes

Locations below describe the implementation plan. README tracks what exists today.

| Pattern            | Problem and planned location                                       | Why needed                                                | Does not solve / common misuse                                                                   |
|--------------------|--------------------------------------------------------------------|-----------------------------------------------------------|--------------------------------------------------------------------------------------------------|
| Saga               | Local steps in Order, Inventory, Payment, Shipping                 | Complete or compensate a workflow across owners           | No global rollback or isolation; cancellation before refund/release is incorrect                 |
| Outbox             | State plus publication intent in each event-producing database     | Survive the DB/Kafka dual-write crash window              | Does not eliminate duplicates; publishing only after commit without durable intent loses events  |
| Event sourcing     | Order state reconstructed from its append-only domain_event stream | Explain and replay business history                       | Not a Kafka topic or audit table alongside an authoritative mutable row                          |
| CQRS               | Commands in Order, materialized reads in Query                     | Optimize reads independently and avoid request fan-out    | Does not require event sourcing or guarantee immediate freshness                                 |
| Aggregator         | Query details endpoint calls Payment, Inventory, Shipping          | Combine owner responses on demand                         | Does not create a transactionally consistent snapshot; unrestricted fan-out worsens availability |
| Circuit breaker    | Cart catalog and query owner lookups                                            | Stop pressure on a failing dependency                     | Does not retry; an open circuit must not manufacture authoritative prices                        |
| Retry              | Transient HTTP, publication and consumption failures               | Recover from temporary problems                           | Cannot fix a decline or malformed event; nested retries cause retry storms                       |
| Idempotency        | Checkout/payment command keys, per-service consumer inboxes        | Make repetitions safe                                     | Event ID deduplication alone misses two distinct events requesting the same charge               |
| Local transaction  | Each application service writes its own database                   | Atomic business change, inbox marker and outbox insertion | An annotation cannot span remote services; network calls inside transactions hold locks          |
| Optimistic locking | Versioned inventory rows and order event versions                  | Detect lost updates without serializing every reader      | Conflict retry must re-read and recheck business invariants                                      |

## Interview questions and strong answers

**Why a saga instead of a distributed transaction?** A saga commits separate local
transactions and uses business compensation when later steps fail. Participants remain
autonomous, but intermediate states are visible. Compensation can fail and must itself
be durable, idempotent and recoverable. It is not database rollback.

**Why do we need an outbox?** A crash can occur after saving an order but before Kafka
publication, or after publication but before a database commit. Insert the publication
intent with business state in one transaction, then relay it. A relay crash after publish
causes duplicates, so consumers still need idempotency.

**Does CQRS require event sourcing?** No. CQRS separates write and read responsibilities.
The write model can be ordinary relational state. Event sourcing instead chooses event
history as the source of truth from which aggregate state is reconstructed.

**What makes event sourcing more than an audit log?** Current state must be derivable
from the ordered event stream. Appends enforce expected aggregate version. Schema
evolution and deterministic replay are part of correctness, not optional reporting.

**Does the same Kafka key guarantee saga ordering?** Only within one topic partition.
An inventory and payment event with the same order key can reach a subscriber in reverse
causal order across topics. Store facts and enforce prerequisites instead of comparing
unrelated aggregate version numbers or timestamp ordering.

**Does Kafka exactly-once processing prevent duplicate charges?** Kafka transactions can
coordinate Kafka reads and writes. They cannot atomically include an arbitrary payment
provider or PostgreSQL commit. Business identity constraints and provider idempotency
are needed even with an idempotent Kafka producer.

**How do HTTP and consumer idempotency differ?** HTTP keys identify repeated commands
and bind to a customer plus request fingerprint. Consumer event IDs identify redelivery
of the same event. Both are durable. Separate business uniqueness constraints prevent
duplicate effects from different IDs representing the same operation.

**How does a circuit breaker differ from retry?** Retry repeats selected failed calls.
A breaker measures outcomes in CLOSED, rejects calls in OPEN, then admits bounded probes
in HALF_OPEN. Successful probes close it. Timeout and bulkhead budgets limit resources;
a fallback is safe for optional descriptions, dangerous for price or payment approval.

**How does optimistic locking stop overselling?** Two transactions may read stock one.
The first update advances the version. The second update affects no row and rolls back,
then reloads stock zero and rejects the reservation. Retrying the stale entity defeats
the design. All order lines must reserve or roll back together.

**Why not one global @Transactional?** Spring's local transaction manager controls one
resource boundary. HTTP and independently owned databases are outside it. Global XA
requires participating resources and coordination, reducing availability and autonomy.

**When choose aggregation over a CQRS projection?** Aggregation is useful for occasional
owner-specific details with explicit partial responses. Projections suit high-volume
queries needing low latency and availability despite owner outages. Neither supplies a
single globally consistent snapshot unless a separate protocol is designed.
