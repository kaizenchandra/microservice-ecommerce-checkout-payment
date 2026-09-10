package com.synechisveltiosi.inventoryservice.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.synechisveltiosi.platform.contracts.Money;

import java.util.*;

/**
 * Consumer-owned mapping of OrderCreated; no dependency on the order-service model.
 */
public final class InventoryEvents {
    private InventoryEvents() {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Line(UUID productId, int quantity) {
        public Line {
            Objects.requireNonNull(productId);
            if (quantity < 1 || quantity > 99) throw new IllegalArgumentException("Invalid reservation quantity");
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record OrderCreated(UUID orderId, UUID customerId, List<Line> items,
                               Money total, String paymentToken, Address shippingAddress) {
        public OrderCreated {
            Objects.requireNonNull(orderId);
            Objects.requireNonNull(customerId);
            Objects.requireNonNull(total);
            Objects.requireNonNull(shippingAddress);
            if (total.amount().signum() <= 0 || !total.currency().equals(Currency.getInstance("USD")))
                throw new IllegalArgumentException("Invalid order total");
            if (paymentToken == null || !Set.of("tok_success", "tok_declined", "tok_timeout", "tok_error").contains(paymentToken))
                throw new IllegalArgumentException("Invalid demo token");
            if (items == null || items.isEmpty() || items.size() > 50)
                throw new IllegalArgumentException("Invalid reservation lines");
            items = items.stream().sorted(Comparator.comparing(Line::productId)).toList();
            if (items.stream().map(Line::productId).distinct().count() != items.size())
                throw new IllegalArgumentException("Duplicate product");
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Address(String recipient, String line1, String city, String postalCode, String country) {
        public Address {
            if (!text(recipient, 100) || !text(line1, 200) || !text(city, 100) || !text(postalCode, 20)
                    || country == null || !country.matches("[A-Z]{2}"))
                throw new IllegalArgumentException("Invalid demo address");
        }

        private static boolean text(String text, int max) {
            return text != null && !text.isBlank() && text.length() <= max;
        }
    }

    public record Reserved(UUID orderId, UUID customerId, List<Line> items, Money total,
                           String paymentToken, Address shippingAddress) {
    }

    public record Outcome(UUID orderId, UUID customerId, List<Line> items, String reason) {
    }
}
