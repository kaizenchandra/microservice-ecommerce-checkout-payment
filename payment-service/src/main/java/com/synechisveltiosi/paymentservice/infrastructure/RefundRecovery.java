package com.synechisveltiosi.paymentservice.infrastructure;

import com.synechisveltiosi.paymentservice.application.RefundWorker;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@Configuration
@EnableScheduling
@ConditionalOnProperty(name = "payment.recovery.enabled", havingValue = "true", matchIfMissing = true)
public class RefundRecovery {
    private final RefundWorker worker;

    public RefundRecovery(RefundWorker worker) {
        this.worker = worker;
    }

    @Scheduled(fixedDelayString = "${payment.recovery.poll-delay-ms:1000}")
    public void poll() {
        try {
            for (int i = 0; i < 10 && worker.processOne(); i++) {
            }
        } catch (Exception error) {
            org.slf4j.LoggerFactory.getLogger(getClass()).warn("Refund recovery retained ({})", error.getClass().getSimpleName());
        }
    }
}
