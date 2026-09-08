package com.synechisveltiosi.paymentservice.domain;

import java.util.UUID;

public record ChargeResult(String status, UUID providerReference) {
    public ChargeResult {
        if (!"COMPLETED".equals(status) && !"FAILED".equals(status)) throw new IllegalArgumentException("A charge result must be terminal");
        if ("COMPLETED".equals(status) && providerReference == null) throw new IllegalArgumentException("Completed charge needs a reference");
    }
}
