package com.synechisveltiosi.orderservice.infrastructure;

import com.synechisveltiosi.orderservice.application.ApiException;
import com.synechisveltiosi.orderservice.application.CommandMetadata;
import com.synechisveltiosi.orderservice.domain.OrderAggregate;
import com.synechisveltiosi.orderservice.domain.OrderEvents;
import com.synechisveltiosi.platform.contracts.EventEnvelope;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** JDBC makes append-only SQL and expected-version compare-and-swap explicit. */
@Repository
public class OrderEventStore {
    private final JdbcTemplate jdbc;
    private final EventCodec codec;
    public OrderEventStore(JdbcTemplate jdbc, EventCodec codec) { this.jdbc = jdbc; this.codec = codec; }

    @Transactional(propagation = Propagation.MANDATORY)
    public void createStream(UUID id) {
        jdbc.update("INSERT INTO order_stream(aggregate_id, current_version) VALUES (?, 0)", id);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public EventEnvelope<OrderEvents.Event> append(UUID id, long expectedVersion, OrderEvents.Event event, CommandMetadata metadata) {
        int changed = jdbc.update("UPDATE order_stream SET current_version = current_version + 1 WHERE aggregate_id = ? AND current_version = ?",
                id, expectedVersion);
        if (changed != 1) { throw new ApiException(HttpStatus.CONFLICT, "STALE_VERSION", "Order changed; reload before updating"); }
        var envelope = new EventEnvelope<OrderEvents.Event>(UUID.randomUUID(), codec.type(event), metadata.correlationId(),
                metadata.causationId(), "Order", id, expectedVersion + 1, Instant.now(), 1, metadata.traceparent(), event);
        String json = codec.encode(envelope);
        jdbc.update("""
                INSERT INTO domain_event(event_id, aggregate_id, aggregate_type, aggregate_version, event_type, schema_version, payload, occurred_at)
                VALUES (?, ?, 'Order', ?, ?, 1, CAST(? AS jsonb), ?)
                """, envelope.eventId(), id, envelope.aggregateVersion(), envelope.eventType(), json, Timestamp.from(envelope.occurredAt()));
        jdbc.update("""
                INSERT INTO outbox_event(id, aggregate_type, aggregate_id, aggregate_version, event_type, topic, payload, created_at)
                VALUES (?, 'Order', ?, ?, ?, 'order.events', CAST(? AS jsonb), ?)
                """, envelope.eventId(), id, envelope.aggregateVersion(), envelope.eventType(), json, Timestamp.from(envelope.occurredAt()));
        return envelope;
    }

    public List<EventEnvelope<OrderEvents.Event>> history(UUID id) {
        return jdbc.query("SELECT payload::text FROM domain_event WHERE aggregate_id = ? ORDER BY aggregate_version",
                (row, index) -> codec.decode(row.getString(1)), id);
    }

    public OrderAggregate load(UUID id) {
        var events = history(id);
        if (events.isEmpty()) { throw new ApiException(HttpStatus.NOT_FOUND, "ORDER_NOT_FOUND", "Order not found"); }
        return OrderAggregate.replay(id, events);
    }
}
