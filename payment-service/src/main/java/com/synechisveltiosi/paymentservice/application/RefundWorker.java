package com.synechisveltiosi.paymentservice.application;
import com.synechisveltiosi.paymentservice.infrastructure.RefundLedger;
import org.springframework.stereotype.Service;
@Service
public class RefundWorker {
    private final RefundTransactions refunds; private final RefundLedger ledger;
    public RefundWorker(RefundTransactions refunds, RefundLedger ledger) { this.refunds = refunds; this.ledger = ledger; }
    public boolean processOne() {
        if (org.springframework.transaction.support.TransactionSynchronizationManager.isActualTransactionActive()) throw new IllegalStateException("Refund worker requires no business transaction");
        var pending = refunds.claim(); if (pending.isEmpty()) return false;
        var claim = pending.get();
        try {
            var reference = ledger.lookup(claim.orderId()).orElseGet(() -> {
                var result = ledger.attempt(claim.orderId(), claim.order().shippingAddress().postalCode());
                if (result.responseLost() || result.reference() == null) throw new IllegalStateException("Refund outcome unresolved");
                return result.reference();
            });
            refunds.finish(claim, reference);
        } catch (Exception error) { refunds.defer(claim, error.getClass().getSimpleName()); }
        return true;
    }
}
