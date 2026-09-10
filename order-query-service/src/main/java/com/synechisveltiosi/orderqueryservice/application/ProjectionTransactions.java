package com.synechisveltiosi.orderqueryservice.application;

import com.synechisveltiosi.orderqueryservice.infrastructure.ProjectionCodec;
import com.synechisveltiosi.platform.contracts.EventEnvelope;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.JsonNode;

import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;

@Service
public class ProjectionTransactions {
    private final JdbcTemplate jdbc;
    private final ProjectionCodec codec;
    private final ProjectionBuilder builder;

    public ProjectionTransactions(JdbcTemplate jdbc, ProjectionCodec codec, ProjectionBuilder builder) {
        this.jdbc = jdbc;
        this.codec = codec;
        this.builder = builder;
    }

    @Transactional
    public void accept(EventEnvelope<JsonNode> event) {
        // All journal writers take this lock BEFORE allocating a sequence, preserving commit order.
        var control = jdbc.queryForMap("SELECT * FROM projection_control WHERE singleton FOR UPDATE");
        String hash = codec.hash(event);
        var inbox = jdbc.queryForList("SELECT content_hash FROM projection_inbox WHERE event_id = ?", event.eventId());
        if (!inbox.isEmpty()) {
            if (!hash.equals(inbox.getFirst().get("content_hash")))
                throw new IllegalArgumentException("Event identity changed");
            return;
        }
        var prior = jdbc.queryForList("SELECT sequence, content_hash FROM projection_event WHERE aggregate_type = ? AND order_id = ? AND aggregate_version = ?",
                event.aggregateType(), event.aggregateId(), event.aggregateVersion());
        if (!prior.isEmpty()) {
            var row = prior.getFirst();
            if (!hash.equals(row.get("content_hash")))
                throw new IllegalArgumentException("Producer version changed content");
            jdbc.update("INSERT INTO projection_inbox VALUES (?, ?, ?)", event.eventId(), hash, row.get("sequence"));
            return;
        }
        long sequence = jdbc.queryForObject("""
                INSERT INTO projection_event(event_id, order_id, aggregate_type, aggregate_version, content_hash, envelope)
                VALUES (?, ?, ?, ?, ?, CAST(? AS jsonb)) RETURNING sequence
                """, Long.class, event.eventId(), event.aggregateId(), event.aggregateType(), event.aggregateVersion(), hash, codec.encode(event));
        jdbc.update("INSERT INTO projection_inbox VALUES (?, ?, ?)", event.eventId(), hash, sequence);
        long generation = ((Number) control.get("active_generation")).longValue();
        project(generation, event.aggregateId(), sequence);
        jdbc.update("UPDATE projection_generation SET last_sequence = ? WHERE id = ?", sequence, generation);
    }

    private void project(long generation, UUID order, long sequence) {
        var history = jdbc.query("SELECT envelope::text FROM projection_event WHERE order_id = ? AND sequence <= ? ORDER BY sequence",
                (row, index) -> codec.decode(row.getString(1)), order, sequence);
        var projected = builder.build(order, history);
        if (projected.isEmpty()) return;
        var view = projected.get();
        jdbc.update("""
                INSERT INTO order_projection(generation, order_id, customer_id, status, order_version, created_at, view)
                VALUES (?, ?, ?, ?, ?, ?, CAST(? AS jsonb))
                ON CONFLICT (generation, order_id) DO UPDATE SET customer_id = EXCLUDED.customer_id, status = EXCLUDED.status,
                order_version = EXCLUDED.order_version, created_at = EXCLUDED.created_at, view = EXCLUDED.view
                """, generation, order, view.customerId(), view.status(), view.orderVersion(), Timestamp.from(view.createdAt()), codec.encode(view));
    }

    @Transactional
    public Rebuild startRebuild() {
        var control = jdbc.queryForMap("SELECT * FROM projection_control WHERE singleton FOR UPDATE");
        if (control.get("building_generation") != null)
            throw new ApiException(HttpStatus.CONFLICT, "REBUILD_ACTIVE", "A projection rebuild is already running");
        long generation = jdbc.queryForObject("INSERT INTO projection_generation(state) VALUES ('BUILDING') RETURNING id", Long.class);
        jdbc.update("UPDATE projection_control SET building_generation = ? WHERE singleton", generation);
        return new Rebuild(generation, "BUILDING");
    }

    @Transactional
    public boolean rebuildStep(int batchSize) {
        if (batchSize < 1 || batchSize > 1000) throw new IllegalArgumentException("Rebuild batch must be 1–1000");
        var control = jdbc.queryForMap("SELECT * FROM projection_control WHERE singleton FOR UPDATE");
        if (control.get("building_generation") == null) return false;
        long generation = ((Number) control.get("building_generation")).longValue();
        long cursor = jdbc.queryForObject("SELECT last_sequence FROM projection_generation WHERE id = ?", Long.class, generation);
        var rows = jdbc.queryForList("SELECT sequence, order_id FROM projection_event WHERE sequence > ? ORDER BY sequence LIMIT ?", cursor, batchSize);
        for (var row : rows) {
            cursor = ((Number) row.get("sequence")).longValue();
            project(generation, (UUID) row.get("order_id"), cursor);
        }
        jdbc.update("UPDATE projection_generation SET last_sequence = ? WHERE id = ?", cursor, generation);
        // Ingestion uses this same lock. No event can commit between catch-up and pointer switch.
        if (rows.size() < batchSize) {
            jdbc.update("UPDATE projection_generation SET state = 'RETIRED' WHERE id = ?", control.get("active_generation"));
            jdbc.update("UPDATE projection_generation SET state = 'ACTIVE', completed_at = clock_timestamp() WHERE id = ?", generation);
            jdbc.update("UPDATE projection_control SET active_generation = ?, building_generation = NULL WHERE singleton", generation);
        }
        return true;
    }

    @Transactional
    public void failRebuild(long generation, String error) {
        var control = jdbc.queryForMap("SELECT * FROM projection_control WHERE singleton FOR UPDATE");
        if (control.get("building_generation") == null || ((Number) control.get("building_generation")).longValue() != generation)
            return;
        jdbc.update("UPDATE projection_generation SET state = 'FAILED', last_error = ?, completed_at = clock_timestamp() WHERE id = ?", error, generation);
        jdbc.update("UPDATE projection_control SET building_generation = NULL WHERE singleton");
    }

    public Long buildingGeneration() {
        return jdbc.queryForObject("SELECT building_generation FROM projection_control WHERE singleton", Long.class);
    }

    @Transactional(readOnly = true, isolation = org.springframework.transaction.annotation.Isolation.REPEATABLE_READ)
    public Status status() {
        var control = jdbc.queryForMap("SELECT * FROM projection_control WHERE singleton");
        long active = ((Number) control.get("active_generation")).longValue();
        var building = control.get("building_generation");
        var recent = jdbc.query("SELECT id, state, last_sequence, last_error FROM projection_generation ORDER BY id DESC LIMIT 20", (row, i) -> new Generation(row.getLong(1), row.getString(2), row.getLong(3), row.getString(4)));
        return new Status(active, building == null ? null : ((Number) building).longValue(), jdbc.queryForObject("SELECT count(*) FROM projection_event", Long.class),
                jdbc.queryForObject("SELECT count(*) FROM order_projection WHERE generation = ?", Long.class, active),
                jdbc.queryForObject("""
                        SELECT count(*) FROM projection_event e LEFT JOIN order_projection p ON p.order_id = e.order_id AND p.generation = ?
                        WHERE e.aggregate_version > COALESCE((p.view->'sourceVersions'->>e.aggregate_type)::bigint, 0)
                        """, Long.class, active), recent);
    }

    public record Rebuild(long generation, String state) {
    }

    public record Generation(long id, String state, long lastSequence, String lastError) {
    }

    public record Status(long activeGeneration, Long buildingGeneration, long journalEvents, long visibleOrders,
                         long bufferedEvents, List<Generation> recentGenerations) {
    }
}
