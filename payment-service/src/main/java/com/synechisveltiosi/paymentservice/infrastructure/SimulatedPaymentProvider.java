package com.synechisveltiosi.paymentservice.infrastructure;

import com.synechisveltiosi.paymentservice.application.PaymentProvider;
import com.synechisveltiosi.paymentservice.domain.ChargeResult;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.Optional;

@Component
public class SimulatedPaymentProvider implements PaymentProvider {
    private final ProviderLedger ledger;

    public SimulatedPaymentProvider(ProviderLedger ledger) {
        this.ledger = ledger;
    }

    private static void requireNoBusinessTransaction() {
        if (TransactionSynchronizationManager.isActualTransactionActive())
            throw new IllegalStateException("Provider calls require a completed business transaction");
    }

    @Override
    public Optional<ChargeResult> reconcile(Request request) {
        requireNoBusinessTransaction();
        return ledger.lookup(request);
    }

    @Override
    public ChargeResult charge(Request request) {
        requireNoBusinessTransaction();
        var attempt = ledger.attempt(request); // Transaction has committed before we simulate response loss.
        if (attempt.responseLost()) throw new ResponseLost();
        if (attempt.unavailable()) throw new ProviderUnavailable();
        return attempt.result();
    }

    public static class ResponseLost extends RuntimeException {
        public ResponseLost() {
            super("Simulated response loss");
        }
    }

    public static class ProviderUnavailable extends RuntimeException {
        public ProviderUnavailable() {
            super("Simulated provider unavailable");
        }
    }
}
