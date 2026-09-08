package com.synechisveltiosi.orderservice.application;

import com.synechisveltiosi.orderservice.api.OrderDtos;
import com.synechisveltiosi.orderservice.domain.OrderAggregate;
import com.synechisveltiosi.orderservice.domain.OrderEvents;
import com.synechisveltiosi.orderservice.infrastructure.EventCodec;
import com.synechisveltiosi.orderservice.infrastructure.OrderEventStore;
import com.synechisveltiosi.platform.contracts.EventEnvelope;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

@Service
public class OrderService {
    private final JdbcTemplate jdbc;
    private final OrderEventStore events;
    private final EventCodec codec;
    private final MeterRegistry metrics;
    public OrderService(JdbcTemplate jdbc, OrderEventStore events, EventCodec codec, MeterRegistry metrics) {
        this.jdbc = jdbc; this.events = events; this.codec = codec; this.metrics = metrics;
    }

    @Transactional
    public OrderDtos.Accepted create(OrderDtos.Create command, UUID key, CommandMetadata metadata) {
        OrderEvents.OrderCreated created;
        try { created = command.event(); }
        catch (IllegalArgumentException error) { throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_ORDER", error.getMessage()); }
        var response = new OrderDtos.Accepted(created.orderId(), 1, OrderAggregate.Status.PENDING, created.total());
        String hash = hash(codec.encode(created));
        int inserted = jdbc.update("""
                INSERT INTO order_command(customer_id, idempotency_key, request_hash, order_id, response)
                VALUES (?, ?, ?, ?, CAST(? AS jsonb)) ON CONFLICT (customer_id, idempotency_key) DO NOTHING
                """, created.customerId(), key, hash, created.orderId(), codec.encode(response));
        if (inserted == 0) {
            var previous = jdbc.queryForMap("SELECT request_hash, response::text FROM order_command WHERE customer_id = ? AND idempotency_key = ?",
                    created.customerId(), key);
            if (!hash.equals(previous.get("request_hash"))) {
                throw new ApiException(HttpStatus.CONFLICT, "IDEMPOTENCY_CONFLICT", "Idempotency key was already used for a different request");
            }
            return codec.read((String) previous.get("response"), OrderDtos.Accepted.class);
        }
        events.createStream(created.orderId());
        events.append(created.orderId(), 0, created, metadata);
        afterCommit(() -> metrics.counter("orders.created").increment());
        return response;
    }

    @Transactional(readOnly = true)
    public OrderDtos.View get(UUID id, UUID customer, boolean admin) {
        return OrderDtos.View.from(owned(id, customer, admin));
    }

    @Transactional
    public OrderDtos.View addNote(UUID id, UUID customer, OrderDtos.AddNote command, CommandMetadata metadata) {
        var aggregate = owned(id, customer, false);
        if (aggregate.version() != command.expectedVersion()) {
            throw new ApiException(HttpStatus.CONFLICT, "STALE_VERSION", "Order changed; reload before updating");
        }
        OrderEvents.OrderNoteAdded event;
        try { event = aggregate.addNote(command.note()); }
        catch (IllegalArgumentException error) { throw new ApiException(HttpStatus.CONFLICT, "NOTE_LIMIT", error.getMessage()); }
        events.append(id, aggregate.version(), event, metadata);
        return OrderDtos.View.from(events.load(id));
    }

    @Transactional(readOnly = true)
    public List<EventEnvelope<OrderEvents.Event>> history(UUID id) { events.load(id); return events.history(id); }

    private OrderAggregate owned(UUID id, UUID customer, boolean admin) {
        var aggregate = events.load(id);
        if (!admin && !aggregate.snapshot().customerId().equals(customer)) {
            throw new ApiException(HttpStatus.NOT_FOUND, "ORDER_NOT_FOUND", "Order not found");
        }
        return aggregate;
    }

    private String hash(String value) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))); }
        catch (NoSuchAlgorithmException error) { throw new IllegalStateException("SHA-256 unavailable", error); }
    }

    public static void afterCommit(Runnable action) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override public void afterCommit() { action.run(); }
        });
    }
}
