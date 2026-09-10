package com.synechisveltiosi.notificationservice.infrastructure;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.concurrent.atomic.AtomicLong;

@Configuration
@EnableScheduling
@ConditionalOnProperty(name = "outbox.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxPolling {
    private static final Logger LOG = LoggerFactory.getLogger(OutboxPolling.class);
    private final OutboxPublisher publisher;
    private final JdbcTemplate jdbc;
    private final int batchSize;
    private final AtomicLong pending = new AtomicLong();
    private final AtomicLong oldestSeconds = new AtomicLong();

    public OutboxPolling(OutboxPublisher publisher, JdbcTemplate jdbc, MeterRegistry metrics,
                         @Value("${outbox.batch-size:10}") int batchSize) {
        if (batchSize < 1 || batchSize > 100) {
            throw new IllegalArgumentException("Outbox batch size must be 1–100");
        }
        this.publisher = publisher;
        this.jdbc = jdbc;
        this.batchSize = batchSize;
        metrics.gauge("outbox.pending", pending);
        metrics.gauge("outbox.oldest.seconds", oldestSeconds);
    }

    @Scheduled(fixedDelayString = "${outbox.poll-delay-ms:1000}")
    public void poll() {
        try {
            for (int i = 0; i < batchSize; i++) {
                if (!publisher.publishOne()) {
                    break;
                }
            }
            var state = jdbc.queryForMap("""
                    SELECT count(*) AS pending,
                    COALESCE(EXTRACT(EPOCH FROM clock_timestamp() - min(created_at)), 0)::bigint AS oldest
                    FROM outbox_event WHERE status = 'PENDING'
                    """);
            pending.set(((Number) state.get("pending")).longValue());
            oldestSeconds.set(((Number) state.get("oldest")).longValue());
        } catch (Exception failure) {
            // No payloads, credentials or SQL parameters in operational error logs.
            LOG.error("Outbox poll failed ({}); retained rows will be retried", failure.getClass().getSimpleName());
        }
    }
}
