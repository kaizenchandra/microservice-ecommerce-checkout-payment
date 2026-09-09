package com.synechisveltiosi.orderqueryservice.application;

import com.synechisveltiosi.orderqueryservice.domain.*;
import com.synechisveltiosi.orderqueryservice.infrastructure.ProjectionCodec;
import com.synechisveltiosi.platform.contracts.EventEnvelope;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.*;

/** A deterministic read-model fold, not another implementation of command-side saga decisions. */
@Component
public class ProjectionBuilder {
    private final ProjectionCodec codec;
    public ProjectionBuilder(ProjectionCodec codec) { this.codec = codec; }
    public Optional<OrderView> build(UUID id, List<EventEnvelope<JsonNode>> history) {
        var streams = new TreeMap<String, List<EventEnvelope<JsonNode>>>();
        for (var event : history) streams.computeIfAbsent(event.aggregateType(), ignored -> new ArrayList<>()).add(event);
        var applied = new TreeMap<String, List<EventEnvelope<JsonNode>>>();
        var versions = new TreeMap<String, Long>();
        for (var entry : streams.entrySet()) {
            entry.getValue().sort(Comparator.comparingLong(EventEnvelope::aggregateVersion));
            long version = 0; var events = new ArrayList<EventEnvelope<JsonNode>>();
            for (var event : entry.getValue()) {
                if (event.aggregateVersion() != version + 1) break;
                events.add(event); version++;
            }
            applied.put(entry.getKey(), events); versions.put(entry.getKey(), version);
        }
        var order = applied.getOrDefault("Order", List.of());
        if (order.isEmpty()) return Optional.empty();
        var first = order.getFirst();
        if (!first.eventType().equals("OrderCreated")) throw new IllegalArgumentException("Order stream must start with creation");
        var created = codec.payload(first, QueryEvents.Created.class);
        if (!id.equals(created.orderId())) throw new IllegalArgumentException("Wrong order identity");
        // Validate even buffered ownership claims once the authoritative order snapshot is available.
        for (var event : history) {
            var customer = event.payload().get("customerId");
            if (customer != null && !created.customerId().toString().equals(customer.asString())) throw new IllegalArgumentException("Inconsistent projection customer");
        }
        String status = "PENDING"; var notes = new ArrayList<OrderView.Note>(); Instant updated = first.occurredAt();
        boolean terminal = false;
        for (var event : order.subList(1, order.size())) {
            updated = event.occurredAt();
            switch (event.eventType()) {
                case "OrderNoteAdded" -> {
                    if (notes.size() >= 20) throw new IllegalArgumentException("Too many order notes");
                    notes.add(new OrderView.Note(codec.payload(event, QueryEvents.Note.class).note(), event.occurredAt()));
                }
                case "OrderFactRecorded" -> {
                    if (terminal) throw new IllegalArgumentException("Fact after terminal order");
                    var fact = codec.payload(event, QueryEvents.Recorded.class).fact();
                    if (Set.of(QueryEvents.Fact.PaymentFailed, QueryEvents.Fact.ShipmentFailed, QueryEvents.Fact.PaymentRefunded).contains(fact)) status = "COMPENSATING";
                }
                case "OrderCompleted", "OrderCancelled" -> {
                    if (terminal) throw new IllegalArgumentException("Multiple terminal outcomes");
                    status = event.eventType().equals("OrderCompleted") ? "COMPLETED" : "CANCELLED"; terminal = true;
                }
                default -> throw new IllegalArgumentException("Invalid order history");
            }
        }
        String inventory = "UNKNOWN", payment = "UNKNOWN", refund = "NONE", shipping = "UNKNOWN", tracking = null;
        UUID providerReference = null, refundReference = null; boolean notified = false;
        for (var entry : applied.entrySet()) if (!entry.getKey().equals("Order")) for (var event : entry.getValue()) {
            switch (event.eventType()) {
                case "InventoryReserved" -> inventory = "RESERVED";
                case "InventoryReservationFailed" -> inventory = "REJECTED";
                case "InventoryReleased" -> {
                    if (!inventory.equals("RESERVED")) throw new IllegalArgumentException("Release without reservation");
                    inventory = "RELEASED";
                }
                case "PaymentCompleted" -> { payment = "COMPLETED"; providerReference = codec.payload(event, QueryEvents.Payment.class).providerReference(); }
                case "PaymentFailed" -> payment = "FAILED";
                case "PaymentRefunded" -> {
                    if (!payment.equals("COMPLETED")) throw new IllegalArgumentException("Refund without completed charge");
                    refund = "REFUNDED"; refundReference = codec.payload(event, QueryEvents.Payment.class).refundReference();
                }
                case "ShipmentCreated" -> { shipping = "CREATED"; tracking = codec.payload(event, QueryEvents.Shipment.class).trackingNumber(); }
                case "ShipmentFailed" -> shipping = "FAILED";
                case "CustomerNotified" -> {
                    var message = codec.payload(event, QueryEvents.Notification.class);
                    if (terminal && !status.equals(message.orderStatus())) throw new IllegalArgumentException("Conflicting notification outcome");
                    notified = true;
                }
                default -> throw new IllegalArgumentException("Unknown projection input");
            }
        }
        return Optional.of(new OrderView(id, created.customerId(), status, versions.get("Order"), created.items(), created.total(), created.shippingAddress(), created.salesChannel(), notes,
                inventory, payment, refund, shipping, tracking, providerReference, refundReference, notified, versions, first.occurredAt(), updated));
    }
}
