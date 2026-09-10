package com.synechisveltiosi.orderqueryservice.domain;

import com.synechisveltiosi.platform.contracts.Money;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public record OrderView(UUID orderId, UUID customerId, String status, long orderVersion,
                        List<QueryEvents.Line> items, Money total, QueryEvents.Address shippingAddress,
                        String salesChannel,
                        List<Note> notes, String inventoryStatus, String paymentStatus, String refundStatus,
                        String shippingStatus, String trackingNumber, UUID providerReference, UUID refundReference,
                        boolean notified, Map<String, Long> sourceVersions, Instant createdAt, Instant updatedAt) {
    public OrderView {
        items = List.copyOf(items);
        notes = List.copyOf(notes);
        sourceVersions = Map.copyOf(sourceVersions);
    }

    public record Note(String text, Instant occurredAt) {
    }
}
