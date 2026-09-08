package com.synechisveltiosi.orderservice.api;

import com.synechisveltiosi.orderservice.domain.OrderAggregate;
import com.synechisveltiosi.orderservice.domain.OrderEvents;
import com.synechisveltiosi.platform.contracts.Money;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

public final class OrderDtos {
    private OrderDtos() { }
    public record Create(@NotNull UUID orderId, @NotNull UUID customerId, @NotNull UUID cartId,
                         @NotNull @PositiveOrZero Long cartVersion,
                         @NotEmpty @Size(max = 50) List<@NotNull OrderEvents.Line> items,
                         @NotNull @Pattern(regexp = "tok_success|tok_declined|tok_timeout|tok_error") String paymentToken,
                         @NotNull OrderEvents.Address shippingAddress,
                         @Pattern(regexp = "WEB|MOBILE") String salesChannel) {
        public OrderEvents.OrderCreated event() {
            var canonical = items.stream().sorted(Comparator.comparing(OrderEvents.Line::productId)).toList();
            return new OrderEvents.OrderCreated(orderId, customerId, cartId, cartVersion, canonical,
                    OrderEvents.total(canonical), paymentToken, shippingAddress, salesChannel);
        }
    }
    public record AddNote(@NotNull @Positive Long expectedVersion, @NotBlank @Size(max = 500) String note) { }
    public record Accepted(UUID orderId, long version, OrderAggregate.Status status, Money total) { }
    public record View(UUID orderId, UUID customerId, long version, OrderAggregate.Status status,
                       List<OrderEvents.Line> items, Money total, OrderEvents.Address shippingAddress,
                       List<OrderAggregate.Note> notes, Instant createdAt, Instant updatedAt) {
        public static View from(OrderAggregate order) {
            return new View(order.id(), order.snapshot().customerId(), order.version(), order.status(),
                    order.snapshot().items(), order.snapshot().total(), order.snapshot().shippingAddress(),
                    order.notes(), order.createdAt(), order.updatedAt());
        }
    }
}
