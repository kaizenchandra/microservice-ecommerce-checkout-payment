package com.synechisveltiosi.orderqueryservice.domain;
import com.synechisveltiosi.platform.contracts.Money;
import java.time.Instant;
import java.util.*;
public record OrderView(UUID orderId, UUID customerId, String status, long orderVersion,
                        List<QueryEvents.Line> items, Money total, QueryEvents.Address shippingAddress, String salesChannel,
                        List<Note> notes, String inventoryStatus, String paymentStatus, String refundStatus,
                        String shippingStatus, String trackingNumber, UUID providerReference, UUID refundReference,
                        boolean notified, Map<String, Long> sourceVersions, Instant createdAt, Instant updatedAt) {
    public record Note(String text, Instant occurredAt) { }
    public OrderView { items = List.copyOf(items); notes = List.copyOf(notes); sourceVersions = Map.copyOf(sourceVersions); }
}
