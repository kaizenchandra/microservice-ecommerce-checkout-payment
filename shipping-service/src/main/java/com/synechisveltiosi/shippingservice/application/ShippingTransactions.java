package com.synechisveltiosi.shippingservice.application;

import com.synechisveltiosi.platform.contracts.EventEnvelope;
import com.synechisveltiosi.shippingservice.domain.ShippingEvents;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HexFormat;
import java.util.UUID;

@Service
public class ShippingTransactions {
    private final JdbcTemplate jdbc;
    private final JsonMapper json;

    public ShippingTransactions(JdbcTemplate jdbc, JsonMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    private static String hash(String text) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
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
    public void accept(EventEnvelope<ShippingEvents.PaymentCompleted> event) {
        var input = event.payload();
        if (!event.eventType().equals("PaymentCompleted") || !event.aggregateType().equals("Payment") || event.schemaVersion() != 1
                || event.aggregateVersion() != 1 || !event.aggregateId().equals(input.orderId()) || !input.paymentId().equals(input.orderId()))
            throw new IllegalArgumentException("Invalid completed payment");
        String hash = hash(json.writeValueAsString(input));
        if (jdbc.update("INSERT INTO processed_event(event_id, request_hash) VALUES (?, ?) ON CONFLICT DO NOTHING", event.eventId(), hash) == 0) {
            if (!hash.equals(jdbc.queryForObject("SELECT request_hash FROM processed_event WHERE event_id = ?", String.class, event.eventId())))
                throw new IllegalArgumentException("Changed event identity");
            return;
        }
        boolean failed = input.shippingAddress().country().equals("ZZ");
        String tracking = failed ? null : "DEMO-" + input.orderId();
        int added = jdbc.update("INSERT INTO shipping(order_id, customer_id, request_hash, status, reference) VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                input.orderId(), input.customerId(), hash, failed ? "FAILED" : "CREATED", tracking);
        if (added == 0) {
            if (!hash.equals(jdbc.queryForObject("SELECT request_hash FROM shipping WHERE order_id = ?", String.class, input.orderId())))
                throw new IllegalArgumentException("Changed shipment instructions");
            return;
        }
        var output = new EventEnvelope<>(UUID.randomUUID(), failed ? "ShipmentFailed" : "ShipmentCreated", event.correlationId(), event.eventId(), "Shipment", input.orderId(),
                1, Instant.now(), 1, com.synechisveltiosi.shippingservice.infrastructure.Telemetry.currentTraceparentOr(event.traceparent()), new ShippingEvents.Outcome(input.orderId(), input.customerId(), tracking, failed ? "UNSUPPORTED_DESTINATION" : null));
        jdbc.update("INSERT INTO outbox_event(id, aggregate_id, aggregate_version, event_type, topic, payload, created_at) VALUES (?, ?, 1, ?, 'shipping.events', CAST(? AS jsonb), ?)",
                output.eventId(), input.orderId(), output.eventType(), json.writeValueAsString(output), Timestamp.from(output.occurredAt()));
    }

    public View get(UUID id) {
        return jdbc.query("SELECT customer_id, status, reference FROM shipping WHERE order_id = ?", (row, i) -> new View(id, row.getObject(1, UUID.class), row.getString(2), row.getString(3)), id)
                .stream().findFirst().orElseThrow(() -> new ApiException(org.springframework.http.HttpStatus.NOT_FOUND, "SHIPMENT_NOT_FOUND", "Shipment not found"));
    }

    public record View(UUID orderId, UUID customerId, String status, String trackingNumber) {
    }
}
