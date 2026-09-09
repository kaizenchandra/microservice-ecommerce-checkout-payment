package com.synechisveltiosi.shippingservice.infrastructure;

import com.synechisveltiosi.shippingservice.application.ShippingTransactions;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Service
public class OutboxPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(OutboxPublisher.class);
    private final JdbcTemplate jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final tools.jackson.databind.json.JsonMapper codec;
    private final MeterRegistry metrics;

    public OutboxPublisher(JdbcTemplate jdbc, KafkaTemplate<String, String> kafka, tools.jackson.databind.json.JsonMapper codec, MeterRegistry metrics) {
        this.jdbc = jdbc; this.kafka = kafka; this.codec = codec; this.metrics = metrics;
    }

    /** One bounded transaction per event, not one transaction for the whole batch. */
    @Transactional(timeout = 10)
    public boolean publishOne() {
        var candidates = jdbc.query("""
                SELECT o.id, o.aggregate_id, o.aggregate_version, o.event_type, o.topic, o.payload::text, o.attempts
                FROM outbox_event o
                WHERE o.status = 'PENDING' AND o.next_attempt_at <= clock_timestamp()
                  AND NOT EXISTS (
                    SELECT 1 FROM outbox_event earlier
                    WHERE earlier.aggregate_id = o.aggregate_id AND earlier.aggregate_version < o.aggregate_version
                      AND earlier.status = 'PENDING')
                ORDER BY o.created_at, o.id
                LIMIT 1 FOR UPDATE OF o SKIP LOCKED
                """, (row, index) -> new Pending(row.getObject("id", UUID.class), row.getObject("aggregate_id", UUID.class),
                row.getLong("aggregate_version"), row.getString("event_type"), row.getString("topic"), row.getString("payload"), row.getInt("attempts")));
        if (candidates.isEmpty()) { return false; }
        Pending row = candidates.getFirst();
        var event = codec.readValue(row.payload(), PublicationMetadata.class);
        var record = new ProducerRecord<String, String>(row.topic(), row.orderId().toString(), row.payload());
        record.headers().add("correlationId", event.correlationId().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add("causationId", event.causationId().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add("eventId", row.id().toString().getBytes(StandardCharsets.UTF_8));
        if (event.traceparent() != null) { record.headers().add("traceparent", event.traceparent().getBytes(StandardCharsets.UTF_8)); }
        try (var correlation = MDC.putCloseable("correlationId", event.correlationId().toString());
             var order = MDC.putCloseable("orderId", row.orderId().toString());
             var eventId = MDC.putCloseable("eventId", row.id().toString());
             var type = MDC.putCloseable("eventType", row.type())) {
            try {
                kafka.send(record).get(5, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Outbox publisher interrupted", interrupted);
            } catch (Exception failure) {
                long delay = Math.min(60000, 1000L << Math.min(row.attempts(), 6)) + ThreadLocalRandom.current().nextLong(250);
                jdbc.update("""
                        UPDATE outbox_event SET attempts = attempts + 1, last_error = ?,
                        next_attempt_at = clock_timestamp() + (? * interval '1 millisecond') WHERE id = ?
                        """, failure.getClass().getSimpleName(), delay, row.id());
                ShippingTransactions.afterCommit(() -> metrics.counter("outbox.publish.failures").increment());
                LOG.warn("Outbox publication deferred; retry delay {} ms", delay);
                return true;
            }
            jdbc.update("UPDATE outbox_event SET status = 'PUBLISHED', published_at = clock_timestamp(), attempts = attempts + 1, last_error = NULL WHERE id = ?", row.id());
            ShippingTransactions.afterCommit(() -> metrics.counter("outbox.published").increment());
            LOG.info("Kafka acknowledged outbox publication; database mark will commit");
        }
        return true;
    }

    public List<Delivery> deliveries(UUID orderId) {
        return jdbc.query("""
                SELECT id, aggregate_version, event_type, status, attempts, next_attempt_at, published_at
                FROM outbox_event WHERE aggregate_id = ? ORDER BY aggregate_version
                """, (row, index) -> new Delivery(row.getObject("id", UUID.class), row.getLong("aggregate_version"),
                row.getString("event_type"), row.getString("status"), row.getInt("attempts"),
                row.getTimestamp("next_attempt_at").toInstant(),
                row.getTimestamp("published_at") == null ? null : row.getTimestamp("published_at").toInstant()), orderId);
    }

    @com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
    private record PublicationMetadata(UUID correlationId, UUID causationId, String traceparent) { }

    private record Pending(UUID id, UUID orderId, long version, String type, String topic, String payload, int attempts) { }
    public record Delivery(UUID eventId, long aggregateVersion, String eventType, String status, int attempts,
                           Instant nextAttemptAt, Instant publishedAt) { }
}
