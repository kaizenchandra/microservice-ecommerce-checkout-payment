package com.synechisveltiosi.inventoryservice.application;

import com.synechisveltiosi.inventoryservice.api.InventoryDtos;
import com.synechisveltiosi.inventoryservice.domain.*;
import com.synechisveltiosi.inventoryservice.infrastructure.StockRepository;
import com.synechisveltiosi.platform.contracts.EventEnvelope;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.*;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

@Service
public class InventoryTransactions {
    private final StockRepository stocks;
    private final JdbcTemplate jdbc;
    private final JsonMapper json;
    private final MeterRegistry metrics;
    public InventoryTransactions(StockRepository stocks, JdbcTemplate jdbc, JsonMapper json, MeterRegistry metrics) {
        this.stocks = stocks; this.jdbc = jdbc; this.json = json; this.metrics = metrics;
    }

    @Transactional
    public InventoryDtos.StockView createStock(InventoryDtos.CreateStock command) {
        // Explicit insert avoids JpaRepository.save merging an existing assigned UUID.
        jdbc.update("INSERT INTO stock(product_id, on_hand, reserved, version) VALUES (?, ?, 0, 0)", command.productId(), command.onHand());
        return InventoryDtos.StockView.from(stock(command.productId()));
    }
    @Transactional(readOnly = true)
    public InventoryDtos.StockView getStock(UUID id) { return InventoryDtos.StockView.from(stock(id)); }
    @Transactional
    public InventoryDtos.StockView setStock(UUID id, InventoryDtos.SetStock command) {
        var stock = stock(id);
        if (stock.version() != command.expectedVersion()) throw conflict("STALE_VERSION", "Stock changed; reload before updating");
        try { stock.setOnHand(command.onHand()); }
        catch (IllegalArgumentException e) { throw conflict("RESERVED_STOCK", e.getMessage()); }
        stocks.flush();
        return InventoryDtos.StockView.from(stock);
    }
    private Stock stock(UUID id) {
        return stocks.findById(id).orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "STOCK_NOT_FOUND", "Stock not found"));
    }

    @Transactional
    public void reserve(EventEnvelope<InventoryEvents.OrderCreated> event) {
        var command = event.payload();
        if (!"OrderCreated".equals(event.eventType()) || !"Order".equals(event.aggregateType()) || event.schemaVersion() != 1
                || event.aggregateVersion() != 1 || !event.aggregateId().equals(command.orderId())) throw new IllegalArgumentException("Invalid order creation envelope");
        String hash = hash(json.writeValueAsString(command));
        if (!process(event.eventId(), "RESERVE:" + hash)) return;
        int inserted = jdbc.update("""
                INSERT INTO reservation(order_id, customer_id, request_hash, status, items)
                VALUES (?, ?, ?, 'PROCESSING', CAST(? AS jsonb)) ON CONFLICT (order_id) DO NOTHING
                """, command.orderId(), command.customerId(), hash, json.writeValueAsString(command.items()));
        if (inserted == 0) {
            String original = jdbc.queryForObject("SELECT request_hash FROM reservation WHERE order_id = ?", String.class, command.orderId());
            if (!hash.equals(original)) throw conflict("RESERVATION_CONFLICT", "Order already has a different reservation request");
            return;
        }
        // Deterministic flush order limits deadlocks when different orders share multiple products.
        var loaded = new ArrayList<Stock>();
        String reason = null;
        for (var line : command.items()) {
            var stock = stocks.findById(line.productId()).orElse(null);
            if (stock == null) { reason = "STOCK_NOT_FOUND"; break; }
            if (stock.available() < line.quantity()) { reason = "INSUFFICIENT_STOCK"; break; }
            loaded.add(stock);
        }
        if (reason != null) {
            jdbc.update("UPDATE reservation SET status = 'REJECTED', version = 1, reason = ? WHERE order_id = ?", reason, command.orderId());
            emit(command.orderId(), 1, "InventoryReservationFailed",
                    new InventoryEvents.Outcome(command.orderId(), command.customerId(), command.items(), reason), event);
            afterCommit(() -> metrics.counter("inventory.reservations.rejected").increment());
            return;
        }
        for (int i = 0; i < loaded.size(); i++) loaded.get(i).reserve(command.items().get(i).quantity());
        stocks.flush(); // A conflict rolls back inbox, all stock updates, reservation and outbox.
        jdbc.update("UPDATE reservation SET status = 'RESERVED', version = 1 WHERE order_id = ?", command.orderId());
        emit(command.orderId(), 1, "InventoryReserved", new InventoryEvents.Reserved(command.orderId(), command.customerId(), command.items(),
                command.total(), command.paymentToken(), command.shippingAddress()), event);
        afterCommit(() -> metrics.counter("inventory.reservations.accepted").increment());
    }

    /** A later compensation consumer can delegate here; missing reservations are retryable, not marked processed. */
    @Transactional
    public InventoryDtos.Reservation release(UUID orderId, UUID commandId, UUID correlationId) {
        return releaseWithReason(orderId, commandId, correlationId, null, "ADMIN_RELEASE");
    }
    @Transactional
    public InventoryDtos.Reservation releaseWithReason(UUID orderId, UUID commandId, UUID correlationId, String traceparent, String reason) {
        var rows = jdbc.queryForList("SELECT order_id FROM reservation WHERE order_id = ? FOR UPDATE", orderId);
        if (rows.isEmpty()) throw new ApiException(HttpStatus.NOT_FOUND, "RESERVATION_NOT_FOUND", "Reservation not found");
        var reservation = reservation(orderId);
        if (!process(commandId, "RELEASE:" + orderId) || !reservation.status().equals("RESERVED")) return reservation;
        for (var line : reservation.items()) stock(line.productId()).release(line.quantity());
        stocks.flush();
        long version = reservation.version() + 1;
        jdbc.update("UPDATE reservation SET status = 'RELEASED', version = ? WHERE order_id = ?", version, orderId);
        var cause = new EventEnvelope<>(commandId, "ReleaseReservation", correlationId, commandId, "InventoryReservation", orderId,
                version, Instant.now(), 1, traceparent, orderId);
        emit(orderId, version, "InventoryReleased", new InventoryEvents.Outcome(orderId, reservation.customerId(), reservation.items(), reason), cause);
        afterCommit(() -> metrics.counter("inventory.reservations.released").increment());
        return reservation(orderId);
    }

    @Transactional(readOnly = true)
    public InventoryDtos.Reservation reservation(UUID id) {
        var rows = jdbc.query("SELECT customer_id, status, version, items::text, reason FROM reservation WHERE order_id = ?",
                (row, index) -> new InventoryDtos.Reservation(id, row.getObject("customer_id", UUID.class), row.getString("status"), row.getLong("version"),
                        List.of(json.readValue(row.getString("items"), InventoryEvents.Line[].class)), row.getString("reason")), id);
        if (rows.isEmpty()) throw new ApiException(HttpStatus.NOT_FOUND, "RESERVATION_NOT_FOUND", "Reservation not found");
        return rows.getFirst();
    }
    private boolean process(UUID id, String hash) {
        String fingerprint = hash(hash);
        if (jdbc.update("INSERT INTO processed_event(event_id, request_hash) VALUES (?, ?) ON CONFLICT DO NOTHING", id, fingerprint) == 1) return true;
        if (!fingerprint.equals(jdbc.queryForObject("SELECT request_hash FROM processed_event WHERE event_id = ?", String.class, id)))
            throw conflict("EVENT_ID_CONFLICT", "Event identity was reused for different content");
        return false;
    }
    private void emit(UUID id, long version, String type, Object payload, EventEnvelope<?> cause) {
        var event = new EventEnvelope<>(UUID.randomUUID(), type, cause.correlationId(), cause.eventId(), "InventoryReservation", id,
                version, Instant.now(), 1, cause.traceparent(), payload);
        jdbc.update("""
                INSERT INTO outbox_event(id, aggregate_id, aggregate_version, event_type, topic, payload, created_at)
                VALUES (?, ?, ?, ?, 'inventory.events', CAST(? AS jsonb), ?)
                """, event.eventId(), id, version, type, json.writeValueAsString(event), Timestamp.from(event.occurredAt()));
    }
    private static String hash(String text) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8))); }
        catch (java.security.NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }
    private static ApiException conflict(String code, String detail) { return new ApiException(HttpStatus.CONFLICT, code, detail); }
    public static void afterCommit(Runnable action) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override public void afterCommit() { action.run(); }
        });
    }
}
