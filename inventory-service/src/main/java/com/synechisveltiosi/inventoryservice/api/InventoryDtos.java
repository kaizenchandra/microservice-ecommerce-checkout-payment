package com.synechisveltiosi.inventoryservice.api;

import com.synechisveltiosi.inventoryservice.domain.Stock;
import com.synechisveltiosi.inventoryservice.domain.InventoryEvents.Line;
import jakarta.validation.constraints.*;
import java.util.List;
import java.util.UUID;

public final class InventoryDtos {
    private InventoryDtos() { }
    public record CreateStock(@NotNull UUID productId, @NotNull @Min(0) @Max(1000000) Integer onHand) { }
    public record SetStock(@NotNull @Min(0) @Max(1000000) Integer onHand, @NotNull @PositiveOrZero Long expectedVersion) { }
    public record StockView(UUID productId, int onHand, int reserved, int available, long version) {
        public static StockView from(Stock stock) { return new StockView(stock.productId(), stock.onHand(), stock.reserved(), stock.available(), stock.version()); }
    }
    public record Reservation(UUID orderId, UUID customerId, String status, long version, List<Line> items, String reason) { }
}
