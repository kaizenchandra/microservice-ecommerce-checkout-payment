package com.synechisveltiosi.orderservice.application;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.synechisveltiosi.orderservice.domain.*;
import com.synechisveltiosi.orderservice.infrastructure.OrderEventStore;
import com.synechisveltiosi.platform.contracts.EventEnvelope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

@Service
public class OrderSaga {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Payload(UUID orderId, UUID customerId) {
        public Payload { Objects.requireNonNull(orderId); Objects.requireNonNull(customerId); }
    }
    private final JdbcTemplate jdbc; private final OrderEventStore events;
    public OrderSaga(JdbcTemplate jdbc, OrderEventStore events) { this.jdbc = jdbc; this.events = events; }
    @Transactional
    public void accept(EventEnvelope<Payload> event) {
        var fact = OrderEvents.Fact.valueOf(event.eventType());
        String producer = switch (fact) {
            case InventoryReserved, InventoryReservationFailed, InventoryReleased -> "InventoryReservation";
            case PaymentCompleted, PaymentFailed, PaymentRefunded -> "Payment";
            case ShipmentCreated, ShipmentFailed -> "Shipment";
        };
        long expected = fact == OrderEvents.Fact.InventoryReleased || fact == OrderEvents.Fact.PaymentRefunded ? 2 : 1;
        if (event.schemaVersion() != 1 || !producer.equals(event.aggregateType()) || event.aggregateVersion() != expected
                || !event.aggregateId().equals(event.payload().orderId())) throw new IllegalArgumentException("Invalid saga envelope");
        var id = event.payload().orderId();
        // Serialize this stream with other appends; facts are not ordered by foreign aggregate versions.
        if (jdbc.queryForList("SELECT aggregate_id FROM order_stream WHERE aggregate_id = ? FOR UPDATE", id).isEmpty())
            throw new IllegalArgumentException("Order has not arrived yet");
        var order = events.load(id);
        if (!order.snapshot().customerId().equals(event.payload().customerId())) throw new IllegalArgumentException("Wrong order customer");
        String hash = hash(event.eventType() + ":" + id + ":" + event.payload().customerId());
        if (jdbc.update("INSERT INTO processed_saga_event(event_id, request_hash) VALUES (?, ?) ON CONFLICT DO NOTHING", event.eventId(), hash) == 0) {
            if (!hash.equals(jdbc.queryForObject("SELECT request_hash FROM processed_saga_event WHERE event_id = ?", String.class, event.eventId()))) throw new IllegalArgumentException("Changed event identity");
            return;
        }
        if (order.hasFact(fact)) return;
        if (order.terminal()) throw new IllegalArgumentException("Unexpected fact after terminal order outcome");
        order.validateFact(fact);
        var metadata = new CommandMetadata(event.correlationId(), event.eventId(), event.traceparent());
        events.append(id, order.version(), new OrderEvents.OrderFactRecorded(fact), metadata);
        order = events.load(id);
        var terminal = order.eligibleTerminal();
        if (terminal != null) {
            OrderEvents.Event outcome = terminal == OrderAggregate.Status.COMPLETED ? new OrderEvents.OrderCompleted(id, order.snapshot().customerId())
                    : new OrderEvents.OrderCancelled(id, order.snapshot().customerId());
            events.append(id, order.version(), outcome, metadata);
        }
    }
    private String hash(String text) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8))); }
        catch (java.security.NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }
}
