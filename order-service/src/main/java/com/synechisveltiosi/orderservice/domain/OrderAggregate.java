package com.synechisveltiosi.orderservice.domain;

import com.synechisveltiosi.platform.contracts.EventEnvelope;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Reconstructed entirely from events; notes do not alter price or shipping instructions. */
public final class OrderAggregate {
    public enum Status { PENDING }
    public record Note(String text, Instant occurredAt) { }
    private final UUID id;
    private long version;
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
    public Status status() { return Status.PENDING; }
    public OrderEvents.OrderCreated snapshot() { return created; }
    public List<Note> notes() { return List.copyOf(notes); }
    public Instant createdAt() { return createdAt; }
    public Instant updatedAt() { return updatedAt; }
}
