package com.synechisveltiosi.orderservice.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.synechisveltiosi.platform.contracts.Money;

import java.math.BigDecimal;
import java.util.*;

public final class OrderEvents {
    private OrderEvents() {
    }

    public static Money total(List<Line> items) {
        return items.stream().map(line -> line.unitPrice().multiply(line.quantity()))
                .reduce(new Money(BigDecimal.ZERO, Currency.getInstance("USD")), Money::add);
    }

    public enum Fact {
        InventoryReserved, InventoryReservationFailed, InventoryReleased,
        PaymentCompleted, PaymentFailed, PaymentRefunded, ShipmentCreated, ShipmentFailed
    }

    public sealed interface Event permits OrderCreated, OrderNoteAdded, OrderFactRecorded, OrderCompleted, OrderCancelled {
    }

    public record Line(UUID productId, String sku, String name, int quantity, Money unitPrice) {
        public Line {
            Objects.requireNonNull(productId);
            if (sku == null || !sku.matches("[A-Z0-9][A-Z0-9-]{1,39}") || name == null || name.isBlank() || name.length() > 160
                    || quantity < 1 || quantity > 99 || unitPrice == null || unitPrice.amount().signum() <= 0
                    || !unitPrice.currency().equals(Currency.getInstance("USD"))
                    || unitPrice.amount().compareTo(new BigDecimal("999999999999.99")) > 0) {
                throw new IllegalArgumentException("Invalid order line");
            }
        }
    }

    public record Address(String recipient, String line1, String city, String postalCode, String country) {
        public Address {
            if (!text(recipient, 100) || !text(line1, 200) || !text(city, 100) || !text(postalCode, 20)
                    || country == null || !country.matches("[A-Z]{2}")) {
                throw new IllegalArgumentException("Invalid shipping address");
            }
        }

        private static boolean text(String value, int max) {
            return value != null && !value.isBlank() && value.length() <= max;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record OrderCreated(UUID orderId, UUID customerId, UUID cartId, long cartVersion,
                               List<Line> items, Money total, String paymentToken, Address shippingAddress,
                               String salesChannel) implements Event {
        public OrderCreated {
            Objects.requireNonNull(orderId);
            Objects.requireNonNull(customerId);
            Objects.requireNonNull(cartId);
            Objects.requireNonNull(shippingAddress);
            if (cartVersion < 0 || items == null || items.isEmpty() || items.size() > 50) {
                throw new IllegalArgumentException("Invalid cart snapshot");
            }
            items = List.copyOf(items);
            Set<UUID> products = new HashSet<>();
            for (Line line : items) {
                if (!products.add(line.productId())) {
                    throw new IllegalArgumentException("Duplicate order product");
                }
            }
            Money calculated = OrderEvents.total(items);
            if (!calculated.equals(total)) {
                throw new IllegalArgumentException("Order total does not match its lines");
            }
            if (!Set.of("tok_success", "tok_declined", "tok_timeout", "tok_error").contains(paymentToken)) {
                throw new IllegalArgumentException("Only fake demo payment tokens are accepted");
            }
            // Backward-compatible schema-1 evolution: older events have no salesChannel.
            salesChannel = salesChannel == null ? "WEB" : salesChannel;
            if (!Set.of("WEB", "MOBILE").contains(salesChannel)) {
                throw new IllegalArgumentException("Invalid sales channel");
            }
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record OrderNoteAdded(UUID customerId, String note) implements Event {
        public OrderNoteAdded {
            Objects.requireNonNull(customerId);
            if (note == null || note.isBlank() || note.length() > 500) {
                throw new IllegalArgumentException("Invalid order note");
            }
        }
    }

    public record OrderFactRecorded(Fact fact) implements Event {
        public OrderFactRecorded {
            Objects.requireNonNull(fact);
        }
    }

    public record OrderCompleted(UUID orderId, UUID customerId) implements Event {
    }

    public record OrderCancelled(UUID orderId, UUID customerId) implements Event {
    }
}
