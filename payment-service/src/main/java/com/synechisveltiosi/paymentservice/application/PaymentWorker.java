package com.synechisveltiosi.paymentservice.application;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Service
public class PaymentWorker {
    private static final Logger LOG = LoggerFactory.getLogger(PaymentWorker.class);
    private final PaymentTransactions transactions;
    private final PaymentProvider provider;
    public PaymentWorker(PaymentTransactions transactions, PaymentProvider provider) { this.transactions = transactions; this.provider = provider; }
    public boolean processOne() {
        if (TransactionSynchronizationManager.isActualTransactionActive()) throw new IllegalStateException("Worker must run outside a transaction");
        var pending = transactions.claim();
        if (pending.isEmpty()) return false;
        var claim = pending.get(); var input = claim.input().payload();
        var request = new PaymentProvider.Request(claim.paymentId(), input.total(), input.paymentToken());
        try {
            // Absence in this strongly consistent simulator permits reusing the SAME provider key.
            var result = provider.reconcile(request).orElseGet(() -> provider.charge(request));
            transactions.finish(claim, result);
        } catch (Exception failure) {
            transactions.defer(claim, failure.getClass().getSimpleName());
            LOG.warn("Payment outcome unresolved; durable recovery retained ({})", failure.getClass().getSimpleName());
        }
        return true;
    }
}
