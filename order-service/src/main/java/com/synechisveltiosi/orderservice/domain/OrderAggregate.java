package com.synechisveltiosi.orderservice.domain;

import com.synechisveltiosi.platform.contracts.EventEnvelope;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Reconstructed entirely from events; notes do not alter price or shipping instructions. */
public final class OrderAggregate {
    public enum Status { PENDING, COMPENSATING, COMPLETED, CANCELLED }
    public record Note(String text, Instant occurredAt) { }
    private final UUID id;
    private long version;
    private Status terminal;
    private final java.util.Set<OrderEvents.Fact> facts = java.util.EnumSet.noneOf(OrderEvents.Fact.class);
    private OrderEvents.OrderCreated created;
    private Instant createdAt;
    private Instant updatedAt;
    private final List<Note> notes = new ArrayList<>();

    private OrderAggregate(UUID id) { this.id = id; }

    public static OrderAggregate replay(UUID id, List<EventEnvelope<OrderEvents.Event>> history) {
        if (history.isEmpty()) { throw new IllegalArgumentException("An order needs an OrderCreated event"); }
        OrderAggregate aggregate = new OrderAggregate(id);
        history.forEach(aggregate::apply);
        return aggregate;
    }

    private void apply(EventEnvelope<OrderEvents.Event> envelope) {
        if (!id.equals(envelope.aggregateId()) || envelope.aggregateVersion() != version + 1) {
            throw new IllegalStateException("Event stream contains a wrong identity or version gap");
        }
        switch (envelope.payload()) {
            case OrderEvents.OrderCreated event -> {
                if (created != null || version != 0 || !id.equals(event.orderId())) { throw new IllegalStateException("Invalid order creation"); }
                created = event;
                createdAt = envelope.occurredAt();
            }
            case OrderEvents.OrderFactRecorded event -> {
                if (created == null || terminal != null || facts.contains(event.fact())) throw new IllegalStateException("Invalid order fact history");
                rejectContradiction(event.fact());
                facts.add(event.fact());
            }
            case OrderEvents.OrderCompleted event -> {
                if (terminal != null || eligibleTerminal() != Status.COMPLETED || !id.equals(event.orderId()) || !created.customerId().equals(event.customerId()))
                    throw new IllegalStateException("Order completion lacks prerequisites");
                terminal = Status.COMPLETED;
            }
            case OrderEvents.OrderCancelled event -> {
                if (terminal != null || eligibleTerminal() != Status.CANCELLED || !id.equals(event.orderId()) || !created.customerId().equals(event.customerId()))
                    throw new IllegalStateException("Order cancellation lacks compensation");
                terminal = Status.CANCELLED;
            }
            case OrderEvents.OrderNoteAdded event -> {
                if (created == null || !created.customerId().equals(event.customerId()) || notes.size() >= 20) {
                    throw new IllegalStateException("Invalid order note history");
                }
                notes.add(new Note(event.note(), envelope.occurredAt()));
            }
        }
        version = envelope.aggregateVersion();
        updatedAt = envelope.occurredAt();
    }

    public OrderEvents.OrderNoteAdded addNote(String note) {
        if (notes.size() >= 20) { throw new IllegalArgumentException("An order may have at most 20 notes"); }
        return new OrderEvents.OrderNoteAdded(created.customerId(), note);
    }
    public UUID id() { return id; }
    public long version() { return version; }
    public Status status() {
        if (terminal != null) return terminal;
        return facts.contains(OrderEvents.Fact.PaymentFailed) || facts.contains(OrderEvents.Fact.ShipmentFailed)
                || facts.contains(OrderEvents.Fact.PaymentRefunded) ? Status.COMPENSATING : Status.PENDING;
    }
    public boolean hasFact(OrderEvents.Fact fact) { return facts.contains(fact); }
    public boolean terminal() { return terminal != null; }
    public void validateFact(OrderEvents.Fact fact) { rejectContradiction(fact); }
    private void rejectContradiction(OrderEvents.Fact fact) {
        for (var pair : java.util.List.of(
                java.util.Set.of(OrderEvents.Fact.InventoryReserved, OrderEvents.Fact.InventoryReservationFailed),
                java.util.Set.of(OrderEvents.Fact.PaymentCompleted, OrderEvents.Fact.PaymentFailed),
                java.util.Set.of(OrderEvents.Fact.ShipmentCreated, OrderEvents.Fact.ShipmentFailed))) {
            if (pair.contains(fact) && pair.stream().anyMatch(other -> other != fact && facts.contains(other))) throw new IllegalArgumentException("Contradictory saga outcome");
        }
    }
    public Status eligibleTerminal() {
        if (facts.contains(OrderEvents.Fact.InventoryReservationFailed)) return Status.CANCELLED;
        if (facts.containsAll(java.util.Set.of(OrderEvents.Fact.InventoryReserved, OrderEvents.Fact.PaymentFailed, OrderEvents.Fact.InventoryReleased))) return Status.CANCELLED;
        if (facts.containsAll(java.util.Set.of(OrderEvents.Fact.InventoryReserved, OrderEvents.Fact.PaymentCompleted, OrderEvents.Fact.ShipmentFailed,
                OrderEvents.Fact.PaymentRefunded, OrderEvents.Fact.InventoryReleased))) return Status.CANCELLED;
        if (facts.containsAll(java.util.Set.of(OrderEvents.Fact.InventoryReserved, OrderEvents.Fact.PaymentCompleted, OrderEvents.Fact.ShipmentCreated))
                && !facts.contains(OrderEvents.Fact.PaymentRefunded) && !facts.contains(OrderEvents.Fact.InventoryReleased)) return Status.COMPLETED;
        return null;
    }
    public OrderEvents.OrderCreated snapshot() { return created; }
    public List<Note> notes() { return List.copyOf(notes); }
    public Instant createdAt() { return createdAt; }
    public Instant updatedAt() { return updatedAt; }
}
