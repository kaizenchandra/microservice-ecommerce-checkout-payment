package com.synechisveltiosi.paymentservice.application;

import com.synechisveltiosi.paymentservice.api.PaymentDtos;
import com.synechisveltiosi.paymentservice.domain.ChargeResult;
import com.synechisveltiosi.paymentservice.domain.PaymentEvents;
import com.synechisveltiosi.platform.contracts.EventEnvelope;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class PaymentTransactions {
    private final JdbcTemplate jdbc;
    private final JsonMapper json;
    private final MeterRegistry metrics;
    private final int leaseSeconds;

    public PaymentTransactions(JdbcTemplate jdbc, JsonMapper json, MeterRegistry metrics,
                               @Value("${payment.recovery.lease-seconds:30}") int leaseSeconds) {
        if (leaseSeconds < 5 || leaseSeconds > 300)
            throw new IllegalArgumentException("Payment lease must be 5–300 seconds");
        this.jdbc = jdbc;
        this.json = json;
        this.metrics = metrics;
        this.leaseSeconds = leaseSeconds;
    }

    public static String hash(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static ApiException conflict(String code, String detail) {
        return new ApiException(HttpStatus.CONFLICT, code, detail);
    }

    public static void afterCommit(Runnable action) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                action.run();
            }
        });
    }

    @Transactional
    public void accept(EventEnvelope<PaymentEvents.InventoryReserved> event) {
        var input = event.payload();
        if (!"InventoryReserved".equals(event.eventType()) || !"InventoryReservation".equals(event.aggregateType())
                || event.schemaVersion() != 1 || event.aggregateVersion() != 1 || !input.orderId().equals(event.aggregateId()))
            throw new IllegalArgumentException("Invalid inventory reservation envelope");
        String hash = hash(json.writeValueAsString(input));
        int marker = jdbc.update("INSERT INTO processed_event(event_id, request_hash) VALUES (?, ?) ON CONFLICT DO NOTHING", event.eventId(), hash);
        if (marker == 0) {
            if (!hash.equals(jdbc.queryForObject("SELECT request_hash FROM processed_event WHERE event_id = ?", String.class, event.eventId())))
                throw conflict("EVENT_ID_CONFLICT", "Event identity was reused with different content");
            return;
        }
        int inserted = jdbc.update("""
                INSERT INTO payment(payment_id, customer_id, request_hash, input_event)
                VALUES (?, ?, ?, CAST(? AS jsonb)) ON CONFLICT DO NOTHING
                """, input.orderId(), input.customerId(), hash, json.writeValueAsString(new EventEnvelope<>(event.eventId(), event.eventType(), event.correlationId(), event.causationId(),
                event.aggregateType(), event.aggregateId(), event.aggregateVersion(), event.occurredAt(), event.schemaVersion(),
                com.synechisveltiosi.paymentservice.infrastructure.Telemetry.currentTraceparentOr(event.traceparent()), event.payload())));
        if (inserted == 0 && !hash.equals(jdbc.queryForObject("SELECT request_hash FROM payment WHERE payment_id = ?", String.class, input.orderId())))
            throw conflict("PAYMENT_CONFLICT", "Order already has different payment instructions");
        if (inserted == 1) afterCommit(() -> metrics.counter("payments.accepted").increment());
    }

    @Transactional(timeout = 5)
    public Optional<Claim> claim() {
        var rows = jdbc.queryForList("""
                SELECT payment_id, input_event::text, attempts FROM payment
                WHERE status IN ('PENDING', 'UNKNOWN') AND next_attempt_at <= clock_timestamp()
                  AND (lease_until IS NULL OR lease_until <= clock_timestamp())
                ORDER BY created_at, payment_id LIMIT 1 FOR UPDATE SKIP LOCKED
                """);
        if (rows.isEmpty()) return Optional.empty();
        var row = rows.getFirst();
        var id = (UUID) row.get("payment_id");
        var token = UUID.randomUUID();
        jdbc.update("""
                UPDATE payment SET status = 'UNKNOWN', attempts = attempts + 1, lease_token = ?,
                lease_until = clock_timestamp() + (? * interval '1 second'), updated_at = clock_timestamp()
                WHERE payment_id = ?
                """, token, leaseSeconds, id);
        return Optional.of(new Claim(id, token, ((Number) row.get("attempts")).intValue() + 1,
                json.readValue((String) row.get("input_event"), new TypeReference<EventEnvelope<PaymentEvents.InventoryReserved>>() {
                })));
    }

    @Transactional
    public boolean finish(Claim claim, ChargeResult result) {
        int changed = jdbc.update("""
                UPDATE payment SET status = ?, version = 1, provider_reference = ?, last_error = NULL,
                lease_token = NULL, lease_until = NULL, updated_at = clock_timestamp()
                WHERE payment_id = ? AND lease_token = ? AND status = 'UNKNOWN'
                """, result.status(), result.providerReference(), claim.paymentId(), claim.token());
        if (changed == 0) return false; // A later worker owns the lease, or terminal state already committed.
        var input = claim.input();
        var order = input.payload();
        boolean success = result.status().equals("COMPLETED");
        String type = success ? "PaymentCompleted" : "PaymentFailed";
        Object payload = success ? new PaymentEvents.Completed(claim.paymentId(), order.orderId(), order.customerId(), order.total(),
                result.providerReference(), order.items(), order.shippingAddress())
                : new PaymentEvents.Failed(claim.paymentId(), order.orderId(), order.customerId(), order.total(), "DECLINED");
        var event = new EventEnvelope<>(UUID.randomUUID(), type, input.correlationId(), input.eventId(), "Payment", claim.paymentId(),
                1, Instant.now(), 1, com.synechisveltiosi.paymentservice.infrastructure.Telemetry.currentTraceparentOr(input.traceparent()), payload);
        jdbc.update("""
                INSERT INTO outbox_event(id, aggregate_id, aggregate_version, event_type, topic, payload, created_at)
                VALUES (?, ?, 1, ?, 'payment.events', CAST(? AS jsonb), ?)
                """, event.eventId(), claim.paymentId(), type, json.writeValueAsString(event), Timestamp.from(event.occurredAt()));
        afterCommit(() -> metrics.counter(success ? "payments.completed" : "payments.declined").increment());
        return true;
    }

    @Transactional
    public void defer(Claim claim, String error) {
        long delay = Math.min(60000L, 1000L << Math.min(claim.attempt() - 1, 6)) + ThreadLocalRandom.current().nextLong(250);
        int changed = jdbc.update("""
                UPDATE payment SET lease_token = NULL, lease_until = NULL, last_error = ?,
                next_attempt_at = clock_timestamp() + (? * interval '1 millisecond'), updated_at = clock_timestamp()
                WHERE payment_id = ? AND lease_token = ? AND status = 'UNKNOWN'
                """, error, delay, claim.paymentId(), claim.token());
        if (changed == 1) afterCommit(() -> metrics.counter("payments.recovery.deferred").increment());
    }

    @Transactional(readOnly = true)
    public PaymentDtos.View get(UUID orderId, UUID customerId, boolean admin) {
        var rows = jdbc.query("SELECT * FROM payment WHERE payment_id = ?", (row, index) -> {
            var input = json.readValue(row.getString("input_event"), new TypeReference<EventEnvelope<PaymentEvents.InventoryReserved>>() {
            });
            return new PaymentDtos.View(orderId, orderId, row.getObject("customer_id", UUID.class), input.payload().total(), row.getString("status"),
                    row.getLong("version"), row.getInt("attempts"), row.getObject("provider_reference", UUID.class), row.getString("last_error"),
                    row.getTimestamp("created_at").toInstant(), row.getTimestamp("updated_at").toInstant());
        }, orderId);
        if (rows.isEmpty() || (!admin && !rows.getFirst().customerId().equals(customerId)))
            throw new ApiException(HttpStatus.NOT_FOUND, "PAYMENT_NOT_FOUND", "Payment not found");
        return rows.getFirst();
    }

    @Transactional
    public PaymentDtos.View retry(UUID id) {
        get(id, null, true);
        jdbc.update("""
                UPDATE payment SET next_attempt_at = clock_timestamp(), updated_at = clock_timestamp()
                WHERE payment_id = ? AND status IN ('PENDING', 'UNKNOWN') AND lease_token IS NULL
                """, id);
        return get(id, null, true);
    }

    public record Claim(UUID paymentId, UUID token, int attempt, EventEnvelope<PaymentEvents.InventoryReserved> input) {
    }
}
