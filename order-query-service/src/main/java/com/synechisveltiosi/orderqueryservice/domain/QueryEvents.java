package com.synechisveltiosi.orderqueryservice.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.synechisveltiosi.platform.contracts.Money;

import java.util.*;

/**
 * Only read-model fields are retained; fake tokens and unused producer fields are discarded.
 */
public final class QueryEvents {
    private QueryEvents() {
    }

    private static void identity(UUID id, UUID customer) {
        Objects.requireNonNull(id);
        Objects.requireNonNull(customer);
    }

    private static boolean text(String value, int max) {
        return value != null && !value.isBlank() && value.length() <= max;
    }

    private static void bounded(String value, int max) {
        if (value != null && value.length() > max) throw new IllegalArgumentException("Oversized field");
    }

    public enum Fact {InventoryReserved, InventoryReservationFailed, InventoryReleased, PaymentCompleted, PaymentFailed, PaymentRefunded, ShipmentCreated, ShipmentFailed}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Line(UUID productId, String sku, String name, int quantity, Money unitPrice) {
        public Line {
            Objects.requireNonNull(productId);
            Objects.requireNonNull(unitPrice);
            if (!text(sku, 40) || !text(name, 160) || quantity < 1 || quantity > 99 || unitPrice.amount().signum() <= 0)
                throw new IllegalArgumentException("Invalid order item");
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Address(String recipient, String line1, String city, String postalCode, String country) {
        public Address {
            if (!text(recipient, 100) || !text(line1, 200) || !text(city, 100) || !text(postalCode, 20) || country == null || !country.matches("[A-Z]{2}"))
                throw new IllegalArgumentException("Invalid address");
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Created(UUID orderId, UUID customerId, List<Line> items, Money total, Address shippingAddress,
                          String salesChannel) {
        public Created {
            Objects.requireNonNull(orderId);
            Objects.requireNonNull(customerId);
            Objects.requireNonNull(total);
            Objects.requireNonNull(shippingAddress);
            if (items == null || items.isEmpty() || items.size() > 50)
                throw new IllegalArgumentException("Invalid order items");
            items = List.copyOf(items);
            if (items.stream().map(Line::productId).distinct().count() != items.size())
                throw new IllegalArgumentException("Duplicate product");
            var calculated = items.stream().map(line -> line.unitPrice().multiply(line.quantity())).reduce(new Money(java.math.BigDecimal.ZERO, Currency.getInstance("USD")), Money::add);
            if (!calculated.equals(total)) throw new IllegalArgumentException("Invalid order total");
            salesChannel = salesChannel == null ? "WEB" : salesChannel;
            if (!Set.of("WEB", "MOBILE").contains(salesChannel))
                throw new IllegalArgumentException("Invalid sales channel");
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Note(UUID customerId, String note) {
        public Note {
            Objects.requireNonNull(customerId);
            if (!text(note, 500)) throw new IllegalArgumentException("Invalid note");
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Recorded(Fact fact) {
        public Recorded {
            Objects.requireNonNull(fact);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Terminal(UUID orderId, UUID customerId) {
        public Terminal {
            identity(orderId, customerId);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Inventory(UUID orderId, UUID customerId, String reason) {
        public Inventory {
            identity(orderId, customerId);
            bounded(reason, 100);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Payment(UUID orderId, UUID customerId, UUID providerReference, UUID refundReference, String reason) {
        public Payment {
            identity(orderId, customerId);
            bounded(reason, 100);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Shipment(UUID orderId, UUID customerId, String trackingNumber, String reason) {
        public Shipment {
            identity(orderId, customerId);
            bounded(trackingNumber, 100);
            bounded(reason, 100);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Notification(UUID orderId, UUID customerId, String orderStatus, String channel) {
        public Notification {
            identity(orderId, customerId);
            if (orderStatus == null || !Set.of("COMPLETED", "CANCELLED").contains(orderStatus) || !"SIMULATED".equals(channel))
                throw new IllegalArgumentException("Invalid notification");
        }
    }
}
