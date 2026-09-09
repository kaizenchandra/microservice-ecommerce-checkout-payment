package com.synechisveltiosi.inventoryservice.infrastructure;
import com.synechisveltiosi.inventoryservice.application.CompensationTransactions;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.*;
@Configuration
@EnableScheduling
@ConditionalOnProperty(name = "compensation.enabled", havingValue = "true", matchIfMissing = true)
public class CompensationRecovery {
    private final CompensationTransactions transactions;
    public CompensationRecovery(CompensationTransactions transactions) { this.transactions = transactions; }
    @Scheduled(fixedDelayString = "${compensation.poll-delay-ms:1000}")
    public void poll() {
        try { for (int i = 0; i < 10 && transactions.processOne(); i++) { } }
        catch (Exception error) { org.slf4j.LoggerFactory.getLogger(getClass()).warn("Inventory compensation retained ({})", error.getClass().getSimpleName()); }
    }
}
