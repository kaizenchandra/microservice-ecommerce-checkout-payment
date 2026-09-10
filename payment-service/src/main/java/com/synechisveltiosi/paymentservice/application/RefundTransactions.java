package com.synechisveltiosi.paymentservice.application;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.synechisveltiosi.paymentservice.domain.PaymentEvents;
import com.synechisveltiosi.platform.contracts.EventEnvelope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Service
public class RefundTransactions {
    private final JdbcTemplate jdbc;
    private final JsonMapper json;

    public RefundTransactions(JdbcTemplate jdbc, JsonMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    @Transactional
    public void accept(EventEnvelope<Input> event) {
        if (!event.eventType().equals("ShipmentFailed") || !event.aggregateType().equals("Shipment") || event.schemaVersion() != 1 || event.aggregateVersion() != 1
                || !event.aggregateId().equals(event.payload().orderId()))
            throw new IllegalArgumentException("Invalid shipment failure");
        int inserted = jdbc.update("INSERT INTO refund(order_id, event_id, customer_id, correlation_id, traceparent) VALUES (?, ?, ?, ?, ?) ON CONFLICT (order_id) DO NOTHING",
                event.aggregateId(), event.eventId(), event.payload().customerId(), event.correlationId(), com.synechisveltiosi.paymentservice.infrastructure.Telemetry.currentTraceparentOr(event.traceparent()));
        if (inserted == 0 && !event.payload().customerId().equals(jdbc.queryForObject("SELECT customer_id FROM refund WHERE order_id = ?", UUID.class, event.aggregateId())))
            throw new IllegalArgumentException("Changed refund customer");
    }

    @Transactional
    public Optional<Claim> claim() {
        var rows = jdbc.queryForList("""
                SELECT r.*, p.input_event::text FROM refund r JOIN payment p ON p.payment_id = r.order_id AND p.customer_id = r.customer_id
                WHERE r.status IN ('PENDING', 'UNKNOWN') AND p.status = 'COMPLETED'
                AND r.next_attempt_at <= clock_timestamp() AND (r.lease_until IS NULL OR r.lease_until <= clock_timestamp())
                ORDER BY r.created_at LIMIT 1 FOR UPDATE OF r SKIP LOCKED
                """);
        if (rows.isEmpty()) return Optional.empty();
        var row = rows.getFirst();
        var id = (UUID) row.get("order_id");
        var token = UUID.randomUUID();
        jdbc.update("UPDATE refund SET status = 'UNKNOWN', attempts = attempts + 1, lease_token = ?, lease_until = clock_timestamp() + interval '30 seconds' WHERE order_id = ?", token, id);
        var order = json.readValue((String) row.get("input_event"), new TypeReference<EventEnvelope<PaymentEvents.InventoryReserved>>() {
        }).payload();
        return Optional.of(new Claim(id, token, ((Number) row.get("attempts")).intValue() + 1, (UUID) row.get("event_id"), (UUID) row.get("customer_id"),
                (UUID) row.get("correlation_id"), (String) row.get("traceparent"), order));
    }

    @Transactional
    public boolean finish(Claim claim, UUID reference) {
        if (jdbc.update("UPDATE refund SET status = 'REFUNDED', provider_reference = ?, lease_token = NULL, lease_until = NULL, last_error = NULL WHERE order_id = ? AND lease_token = ? AND status = 'UNKNOWN'",
                reference, claim.orderId(), claim.token()) == 0) return false;
        var output = new EventEnvelope<>(UUID.randomUUID(), "PaymentRefunded", claim.correlationId(), claim.eventId(), "Payment", claim.orderId(), 2, Instant.now(), 1, com.synechisveltiosi.paymentservice.infrastructure.Telemetry.currentTraceparentOr(claim.traceparent()),
                new Refunded(claim.orderId(), claim.orderId(), claim.customerId(), claim.order().total(), reference));
        jdbc.update("INSERT INTO outbox_event(id, aggregate_id, aggregate_version, event_type, topic, payload, created_at) VALUES (?, ?, 2, 'PaymentRefunded', 'payment.events', CAST(? AS jsonb), ?)",
                output.eventId(), claim.orderId(), json.writeValueAsString(output), Timestamp.from(output.occurredAt()));
        return true;
    }

    @Transactional
    public void defer(Claim claim, String error) {
        long delay = Math.min(60000L, 1000L << Math.min(claim.attempts() - 1, 6)) + java.util.concurrent.ThreadLocalRandom.current().nextLong(250);
        jdbc.update("UPDATE refund SET lease_token = NULL, lease_until = NULL, last_error = ?, next_attempt_at = clock_timestamp() + (? * interval '1 millisecond') WHERE order_id = ? AND lease_token = ? AND status = 'UNKNOWN'",
                error, delay, claim.orderId(), claim.token());
    }

    public View get(UUID id) {
        return jdbc.query("SELECT status, attempts, provider_reference, last_error FROM refund WHERE order_id = ?", (row, i) -> new View(id, row.getString(1), row.getInt(2), row.getObject(3, UUID.class), row.getString(4)), id)
                .stream().findFirst().orElseThrow(() -> new ApiException(org.springframework.http.HttpStatus.NOT_FOUND, "REFUND_NOT_FOUND", "Refund not found"));
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Input(UUID orderId, UUID customerId) {
        public Input {
            Objects.requireNonNull(orderId);
            Objects.requireNonNull(customerId);
        }
    }

    public record Claim(UUID orderId, UUID token, int attempts, UUID eventId, UUID customerId, UUID correlationId,
                        String traceparent, PaymentEvents.InventoryReserved order) {
    }

    public record Refunded(UUID orderId, UUID paymentId, UUID customerId,
                           com.synechisveltiosi.platform.contracts.Money total, UUID refundReference) {
    }

    public record View(UUID orderId, String status, int attempts, UUID providerReference, String lastError) {
    }
}
