package com.synechisveltiosi.paymentservice.infrastructure;

import com.synechisveltiosi.paymentservice.application.PaymentWorker;
import io.micrometer.core.instrument.MeterRegistry;
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
@ConditionalOnProperty(name = "payment.recovery.enabled", havingValue = "true", matchIfMissing = true)
public class PaymentRecovery {
    private final PaymentWorker worker;
    private final JdbcTemplate jdbc;
    private final int batchSize;
    private final AtomicLong unresolved = new AtomicLong();
    private final AtomicLong oldestSeconds = new AtomicLong();

    public PaymentRecovery(PaymentWorker worker, JdbcTemplate jdbc, MeterRegistry metrics,
                           @Value("${payment.recovery.batch-size:10}") int batchSize) {
        if (batchSize < 1 || batchSize > 100) throw new IllegalArgumentException("Recovery batch must be 1–100");
        this.worker = worker;
        this.jdbc = jdbc;
        this.batchSize = batchSize;
        metrics.gauge("payments.unresolved", unresolved);
        metrics.gauge("payments.unresolved.oldest.seconds", oldestSeconds);
    }

    @Scheduled(fixedDelayString = "${payment.recovery.poll-delay-ms:1000}")
    public void recover() {
        try {
            for (int i = 0; i < batchSize && worker.processOne(); i++) {
            }
            var state = jdbc.queryForMap("""
                    SELECT count(*) AS pending, COALESCE(EXTRACT(EPOCH FROM clock_timestamp() - min(created_at)), 0)::bigint AS oldest
                    FROM payment WHERE status IN ('PENDING', 'UNKNOWN')
                    """);
            unresolved.set(((Number) state.get("pending")).longValue());
            oldestSeconds.set(((Number) state.get("oldest")).longValue());
        } catch (Exception failure) {
            LoggerFactory.getLogger(PaymentRecovery.class).error("Payment recovery deferred ({})", failure.getClass().getSimpleName());
        }
    }
}
