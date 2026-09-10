package com.synechisveltiosi.cartservice.api;

import com.synechisveltiosi.cartservice.domain.Cart;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

public final class CartDtos {
    private CartDtos() {
    }

    public record Add(@NotNull UUID productId, @NotNull @Positive @Max(99) Integer quantity,
                      @NotNull @PositiveOrZero Long expectedVersion) {
    }

    public record SetQuantity(@NotNull @Positive @Max(99) Integer quantity,
                              @NotNull @PositiveOrZero Long expectedVersion) {
    }

    public record Item(UUID productId, int quantity) {
    }

    public record View(UUID id, UUID customerId, long version, List<Item> items, Instant createdAt, Instant updatedAt) {
        public static View from(Cart cart) {
            var items = cart.items().entrySet().stream().map(entry -> new Item(entry.getKey(), entry.getValue()))
                    .sorted(Comparator.comparing(Item::productId)).toList();
            return new View(cart.id(), cart.customerId(), cart.version(), items, cart.createdAt(), cart.updatedAt());
        }
    }
}
