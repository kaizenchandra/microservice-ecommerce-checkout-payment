# Phase 9 — Order details composition

`GET /api/order-views/{orderId}/details` returns the owned CQRS `projection` plus
`payment`, `inventory` and `shipping` sections. Use the same demo Basic credentials
as the other query APIs. An unauthenticated request returns 401; a missing or another
customer's projection returns 404 before any remote calls. Admins can read all orders.

Each remote section contains `availability` and `data`:

| Availability | Meaning                                                              |
|--------------|----------------------------------------------------------------------|
| AVAILABLE    | Owner returned a readable result matching the order and customer     |
| NOT_FOUND    | Owner returned 404; the record may not yet exist or never be created |
| UNAVAILABLE  | Timeout, overload, access failure, invalid response or owner failure |

Partial responses return HTTP 200 and preserve the projection. Unavailable and missing
sections have null data. Missing payment or shipment is not evidence of failure or
success: an inventory-rejected order can legitimately have neither. These independent
reads are not a transactionally consistent snapshot and can be newer than the projection.
The endpoint does not change orders, trigger payments or retry remote calls.

Payment details include status, payment identity and provider reference. Inventory
includes status, version and reason. Shipping includes status and tracking number.
The query service uses local response records and discards unknown fields, including
tokens and owner diagnostics. The projection continues to supply priced order items.

Three lookups execute concurrently with one shared one-second response deadline and
one-second connect/read limits. A process-wide limit allows 24 active owner lookups;
excess lookups report UNAVAILABLE without queuing for a permit. Cancellation interrupts
unfinished work, and permits remain held until that work exits. Configure
`DETAILS_TIMEOUT_MS` (1–10000) and `DETAILS_MAX_CONCURRENT_LOOKUPS` (3–300).
Owner URLs use `PAYMENT_SERVICE_URL`, `INVENTORY_SERVICE_URL` and `SHIPPING_SERVICE_URL`;
Compose points them at the service network names.

Caller Authorization, X-Correlation-ID and optional traceparent are forwarded to the
configured owners. Redirects are disabled. Inventory and shipping provide new read-only
`GET /api/inventory/reservations/{orderId}/details` and
`GET /api/shipping/{orderId}/details` routes, each independently checking ownership.
Existing admin operations retain their access restrictions.

Example using the existing synthetic order (replace the ID for another owned order):

```sh
curl --fail-with-body -u "11111111-1111-1111-1111-111111111111:${DEMO_CUSTOMER_PASSWORD}" \
  http://localhost:8080/api/order-views/4d124df9-8bee-48ec-97db-bdac907d4fdb/details
```

## Verification

HTTP fixture tests cover concurrent fan-out, caller context, partial failures, malformed
responses, mismatched ownership, redirects, deadlines and concurrency limits. A controller
test checks ownership rejection before fan-out. The packaged-service saga suite checks
composition through the gateway with real payment, inventory and shipping data, including
owner/admin access, unauthenticated rejection and cross-customer rejection at both the
query and owner APIs.

Java 21 `mvn -o -pl order-query-service,saga-tests -am verify` passed after adding
the HTTP client starter. The preceding full reactor run passed the unaffected modules;
current Surefire/Failsafe reports total 73 tests with zero failures, errors or skips.
Compose configuration and whitespace checks passed. Shared-stack deployment has not
been performed for Phase 9.
