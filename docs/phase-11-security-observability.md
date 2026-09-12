# Phase 11 — JWT identity, tracing and business metrics

## Local JWT authentication

The gateway and every application validate bearer JWTs independently. The gateway
forwards the token; services retain their existing ownership and role checks. Customer
identity comes from the validated subject, never a customer header or request parameter.
Order submission remains restricted to CHECKOUT or ADMIN; checkout HTTP orchestration
is still pending.

Run the environment initializer to append the missing signing key without rotating
existing credentials, then issue a short-lived token locally:

```bash
python3 infrastructure/scripts/init-env.py
TOKEN=$(python3 infrastructure/scripts/demo_token.py --identity customer)
curl --fail-with-body -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/api/order-views
unset TOKEN
```

The issuer supports `customer`, `other`, `admin` and `checkout`. Tokens default to 15
minutes; `--ttl` accepts 1–3600 seconds. The key is read from `JWT_SECRET` in the
process environment or the ignored `.env` file. Only the local issuer prints the token;
application logs and verification scripts do not print it. Access to this key grants
issuer authority: this is an operator-controlled demo tool, not a public login endpoint.

Validation pins HS256, requires a signing key of at least 32 bytes, checks issuer
`checkout-demo` and audience `ecommerce-api`, validates expiry/not-before with 30 seconds
of clock skew, and requires issued-at and expiry with a maximum one-hour lifetime.
Exactly one supported role is required. CUSTOMER subjects must be canonical UUIDs;
ADMIN and CHECKOUT subjects are `admin` and `checkout`. Missing/invalid claims,
wrong keys, expired tokens and different algorithms are rejected. The issuer and
audience can be configured through JWT_ISSUER/JWT_AUDIENCE or Spring properties.

The local demo shares an HMAC key across applications; a service with that key can also
sign tokens. Production requires an external issuer, asymmetric verification/JWKS,
rotation and an appropriate trust model. No production identity-provider integration
or refresh-token flow is claimed.

Basic authentication is disabled by default. Existing isolated service tests explicitly
set `demo.auth.basic-enabled=true`; the packaged saga suite uses JWT with Basic disabled.
An operator can temporarily opt a legacy service into Basic with
`DEMO_AUTH_BASIC_ENABLED=true` (Spring relaxed binding). The gateway has no Basic adapter.
Older curl examples in Phases 3–10 describe the earlier deployment; on Phase 11 replace
`-u` with the bearer header above. The catalog/cart and order verification scripts now
mint JWTs internally.

## Real trace propagation and export

Each service owns an OpenTelemetry SDK tracer and exports OTLP/HTTP to the configured
collector (`MANAGEMENT_OTLP_TRACING_ENDPOINT`, default localhost:4318/v1/traces).
Compose points at `otel-collector:4318/v1/traces`, which forwards to Tempo. Export uses
a bounded queue of 2048 spans, batches of at most 128, a 200 ms schedule and a two-second
network timeout. Export failures do not change business results. Queued spans can be
lost on crashes or overload. `TELEMETRY_EXPORT_ENABLED=false` disables export for tests
or local work without a collector. Incoming sampling flags are respected.

HTTP filters validate correlation/trace headers, create a server span, establish MDC
traceId/spanId/correlationId, and forward the server context to downstream calls. A
missing correlation ID is generated. The response includes traceparent and
X-Correlation-ID. Actuator health/metrics traffic is excluded from custom HTTP spans.
Span names are bounded (`http.server`, `kafka.publish`, `kafka.consume`); URL parameters,
Authorization headers and event payloads are not exported as attributes.

Outbox publishers create producer spans from the persisted event context, inject their
new span context in Kafka headers, and wait for broker acknowledgment inside the span.
Consumer interceptors create child spans around listener work and restore thread/MDC
state afterward, including failure paths. When a Kafka trace header is absent, such as
a redriven record, the consumer falls back to the envelope's stored traceparent.
Events and durable payment/refund work retain the processing context so subsequent
workers and outbox polls can continue the trace after a process restart. Retries and
replays create new spans while preserving the business event's identity.

ECS consumer logs include service identity and trace/span/correlation/event context;
existing outbox logs include order and event metadata. These identifiers belong in
logs/traces, never metric labels. Use the trace ID in Grafana's Tempo datasource to
inspect service-to-service work. The trace's parent relationships reflect asynchronous
publication and consumption rather than implying one database transaction.

## Metrics and dashboard

Existing after-commit business counters report created orders, accepted/rejected/released
inventory reservations, accepted/completed/declined payments and deferred payment work.
Outbox counters track acknowledged publication and failures; DLT counters track recovery.
`messaging.processed{outcome="success|failure"}` measures listener attempts, including
retries and duplicates, rather than unique business effects.

The Grafana platform dashboard adds business throughput, payment outcomes, listener
outcomes and DLT recovery panels. Labels are bounded application/outcome/metric names;
order/customer/event IDs are excluded. Prometheus continues scraping the existing
service-network endpoints. Only the gateway is host-published, on loopback; its own
Prometheus endpoint is also reachable there. These local endpoints are not an internet
exposure configuration.

## Verification

JWT tests cover signatures, pinned algorithms, required claims, roles, subject mapping,
expiry/not-before, audience, issuer and weak-key rejection. The actual Python issuer
is exercised against the gateway and owner APIs. The packaged saga suite runs with
JWT and Basic disabled, preserving ownership, compensation and idempotency assertions.
A local OTLP receiver decodes exported spans and checks the trace across seven services,
consumer-to-producer parent links, correlated ECS logs and bounded business metrics.
Scope tests check that trace and MDC state are restored after nested work.

Java 21 `mvn -o verify` passed all 13 reactor modules: 86 tests with zero failures,
errors or skips. Compose configuration, script syntax and whitespace checks passed.
Dashboard outcome counters use separate queries and explicit legends to keep success,
failure and deferred-work series distinct. Phase 11 has not been deployed to the
shared Compose stack.

Implementation
references: [Spring Security JWT validation](https://docs.spring.io/spring-security/reference/servlet/oauth2/resource-server/jwt.html)
and [OpenTelemetry Java SDK](https://opentelemetry.io/docs/languages/java/sdk/).
