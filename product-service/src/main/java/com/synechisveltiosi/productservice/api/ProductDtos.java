package com.synechisveltiosi.productservice.api;

import com.synechisveltiosi.platform.contracts.Money;
import com.synechisveltiosi.productservice.domain.Product;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Currency;
import java.util.List;
import java.util.UUID;

public final class ProductDtos {
    private ProductDtos() {
    }

    public record Price(@NotNull @DecimalMin("0.01") @Digits(integer = 12, fraction = 2) BigDecimal amount,
                        @NotNull @Pattern(regexp = "USD") String currency) {
        public Money toMoney() {
            return new Money(amount, Currency.getInstance(currency));
        }
    }

    public record Create(@NotBlank @Pattern(regexp = "[A-Z0-9][A-Z0-9-]{1,39}") String sku,
                         @NotBlank @Size(max = 160) String name,
                         @NotNull @Size(max = 2000) String description,
                         @NotNull @Valid Price price) {
    }

    public record Update(@NotBlank @Size(max = 160) String name,
                         @NotNull @Size(max = 2000) String description,
                         @NotNull @Valid Price price, @NotNull Boolean active,
                         @NotNull @PositiveOrZero Long expectedVersion) {
    }

    public record View(UUID id, String sku, String name, String description, Money price, boolean active,
                       long version, Instant createdAt, Instant updatedAt) {
        public static View from(Product product) {
            return new View(product.id(), product.sku(), product.name(), product.description(), product.price(),
                    product.active(), product.version(), product.createdAt(), product.updatedAt());
        }
    }

    public record PageView(List<View> items, int page, int size, long totalElements, int totalPages) {
    }
}
