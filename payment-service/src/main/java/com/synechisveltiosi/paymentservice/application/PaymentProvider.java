package com.synechisveltiosi.paymentservice.application;

import com.synechisveltiosi.paymentservice.domain.ChargeResult;
import com.synechisveltiosi.platform.contracts.Money;
import java.util.Optional;
import java.util.UUID;

public interface PaymentProvider {
    record Request(UUID paymentId, Money total, String paymentToken) {
        public Request {
            java.util.Objects.requireNonNull(paymentId); java.util.Objects.requireNonNull(total);
            if (total.amount().signum() <= 0 || !total.currency().equals(java.util.Currency.getInstance("USD"))
                    || paymentToken == null || !java.util.Set.of("tok_success", "tok_declined", "tok_timeout", "tok_error").contains(paymentToken))
                throw new IllegalArgumentException("Invalid simulated charge request");
        }
    }
    Optional<ChargeResult> reconcile(Request request);
    ChargeResult charge(Request request);
}
