package com.synechisveltiosi.inventoryservice.application;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.synechisveltiosi.platform.contracts.EventEnvelope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;
import java.util.UUID;

@Service
public class CompensationTransactions {
    private final JdbcTemplate jdbc;
    private final InventoryTransactions inventory;
    public CompensationTransactions(JdbcTemplate jdbc, InventoryTransactions inventory) {
        this.jdbc = jdbc;
        this.inventory = inventory;
    }

    @Transactional
    public void accept(EventEnvelope<Payload> event) {
        boolean failed = event.eventType().equals("PaymentFailed");
        if ((!failed && !event.eventType().equals("PaymentRefunded")) || !event.aggregateType().equals("Payment") || event.schemaVersion() != 1
                || event.aggregateVersion() != (failed ? 1 : 2) || !event.aggregateId().equals(event.payload().orderId()))
            throw new IllegalArgumentException("Invalid compensation event");
        int inserted = jdbc.update("INSERT INTO compensation(order_id, event_id, customer_id, event_type, correlation_id, traceparent) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (order_id) DO NOTHING",
                event.payload().orderId(), event.eventId(), event.payload().customerId(), event.eventType(), event.correlationId(), event.traceparent());
        if (inserted == 0) {
            var existing = jdbc.queryForMap("SELECT customer_id, event_type FROM compensation WHERE order_id = ?", event.aggregateId());
            if (!event.payload().customerId().equals(existing.get("customer_id")) || !event.eventType().equals(existing.get("event_type")))
                throw new IllegalArgumentException("Changed compensation instructions");
        }
    }

    @Transactional
    public boolean processOne() {
        var rows = jdbc.queryForList("""
                SELECT c.* FROM compensation c JOIN reservation r USING (order_id)
                WHERE c.status = 'PENDING' ORDER BY c.created_at LIMIT 1 FOR UPDATE OF c SKIP LOCKED
                """);
        if (rows.isEmpty()) return false;
        var row = rows.getFirst();
        var id = (UUID) row.get("order_id");
        var reservation = inventory.reservation(id);
        if (!reservation.customerId().equals(row.get("customer_id")))
            throw new IllegalArgumentException("Wrong compensation customer");
        inventory.releaseWithReason(id, (UUID) row.get("event_id"), (UUID) row.get("correlation_id"), (String) row.get("traceparent"),
                row.get("event_type").equals("PaymentFailed") ? "PAYMENT_FAILED" : "PAYMENT_REFUNDED");
        jdbc.update("UPDATE compensation SET status = 'DONE' WHERE order_id = ?", id);
        return true;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Payload(UUID orderId, UUID customerId) {
        public Payload {
            Objects.requireNonNull(orderId);
            Objects.requireNonNull(customerId);
        }
    }
}
