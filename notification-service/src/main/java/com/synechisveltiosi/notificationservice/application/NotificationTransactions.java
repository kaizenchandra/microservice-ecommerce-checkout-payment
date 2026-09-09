package com.synechisveltiosi.notificationservice.application;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.synechisveltiosi.platform.contracts.EventEnvelope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.*;
import tools.jackson.databind.json.JsonMapper;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
@Service
public class NotificationTransactions {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Payload(UUID orderId, UUID customerId) { public Payload { Objects.requireNonNull(orderId); Objects.requireNonNull(customerId); } }
    public record Delivered(UUID orderId, UUID customerId, String orderStatus, String channel) { }
    private final JdbcTemplate jdbc; private final JsonMapper json;
    public NotificationTransactions(JdbcTemplate jdbc, JsonMapper json) { this.jdbc = jdbc; this.json = json; }
    @Transactional
    public void accept(EventEnvelope<Payload> event) {
        boolean completed = event.eventType().equals("OrderCompleted");
        if ((!completed && !event.eventType().equals("OrderCancelled")) || !event.aggregateType().equals("Order") || event.schemaVersion() != 1
                || !event.aggregateId().equals(event.payload().orderId())) throw new IllegalArgumentException("Invalid terminal order event");
        String hash;
        try { hash = HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest((event.eventType() + event.payload()).getBytes(java.nio.charset.StandardCharsets.UTF_8))); }
        catch (java.security.NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
        if (jdbc.update("INSERT INTO processed_event(event_id, request_hash) VALUES (?, ?) ON CONFLICT DO NOTHING", event.eventId(), hash) == 0) {
            if (!hash.equals(jdbc.queryForObject("SELECT request_hash FROM processed_event WHERE event_id = ?", String.class, event.eventId()))) throw new IllegalArgumentException("Changed notification event");
            return;
        }
        String status = completed ? "COMPLETED" : "CANCELLED";
        int inserted = jdbc.update("INSERT INTO notification(order_id, customer_id, request_hash, status, reference) VALUES (?, ?, ?, ?, 'SIMULATED') ON CONFLICT DO NOTHING",
                event.aggregateId(), event.payload().customerId(), hash, status);
        if (inserted == 0) {
            if (!hash.equals(jdbc.queryForObject("SELECT request_hash FROM notification WHERE order_id = ?", String.class, event.aggregateId()))) throw new IllegalArgumentException("Conflicting terminal notification");
            return;
        }
        var output = new EventEnvelope<>(UUID.randomUUID(), "CustomerNotified", event.correlationId(), event.eventId(), "Notification", event.aggregateId(), 1, Instant.now(), 1,
                event.traceparent(), new Delivered(event.aggregateId(), event.payload().customerId(), status, "SIMULATED"));
        jdbc.update("INSERT INTO outbox_event(id, aggregate_id, aggregate_version, event_type, topic, payload, created_at) VALUES (?, ?, 1, 'CustomerNotified', 'notification.events', CAST(? AS jsonb), ?)",
                output.eventId(), event.aggregateId(), json.writeValueAsString(output), Timestamp.from(output.occurredAt()));
    }
    public Delivered get(UUID id) {
        return jdbc.query("SELECT customer_id, status FROM notification WHERE order_id = ?", (row, i) -> new Delivered(id, row.getObject(1, UUID.class), row.getString(2), "SIMULATED"), id)
                .stream().findFirst().orElseThrow(() -> new ApiException(org.springframework.http.HttpStatus.NOT_FOUND, "NOTIFICATION_NOT_FOUND", "Notification not found"));
    }
    public static void afterCommit(Runnable action) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() { @Override public void afterCommit() { action.run(); } });
    }
}
