package com.synechisveltiosi.paymentservice.api;

import com.synechisveltiosi.platform.contracts.Money;

import java.time.Instant;
import java.util.UUID;

public final class PaymentDtos {
    private PaymentDtos() {
    }

    public record View(UUID paymentId, UUID orderId, UUID customerId, Money total, String status, long version,
                       int attempts, UUID providerReference, String lastError, Instant createdAt, Instant updatedAt) {
    }
}
