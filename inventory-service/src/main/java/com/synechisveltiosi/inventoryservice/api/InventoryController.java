package com.synechisveltiosi.inventoryservice.api;

import com.synechisveltiosi.inventoryservice.application.InventoryService;
import com.synechisveltiosi.inventoryservice.application.InventoryTransactions;
import com.synechisveltiosi.inventoryservice.infrastructure.OutboxPublisher;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/inventory")
public class InventoryController {
    private final InventoryTransactions transactions;
    private final InventoryService inventory;
    private final OutboxPublisher outbox;

    public InventoryController(InventoryTransactions transactions, InventoryService inventory, OutboxPublisher outbox) {
        this.transactions = transactions;
        this.inventory = inventory;
        this.outbox = outbox;
    }

    @PostMapping("/stock")
    public ResponseEntity<InventoryDtos.StockView> create(@Valid @RequestBody InventoryDtos.CreateStock command) {
        var stock = transactions.createStock(command);
        return ResponseEntity.created(URI.create("/api/inventory/stock/" + stock.productId())).body(stock);
    }

    @GetMapping("/stock/{id}")
    public InventoryDtos.StockView stock(@PathVariable UUID id) {
        return transactions.getStock(id);
    }

    @PutMapping("/stock/{id}")
    public InventoryDtos.StockView set(@PathVariable UUID id, @Valid @RequestBody InventoryDtos.SetStock command) {
        return transactions.setStock(id, command);
    }

    @GetMapping("/reservations/{orderId}")
    public InventoryDtos.Reservation reservation(@PathVariable UUID orderId) {
        return transactions.reservation(orderId);
    }

    @PostMapping("/reservations/{orderId}/release")
    public InventoryDtos.Reservation release(@PathVariable UUID orderId, @RequestHeader("Idempotency-Key") UUID key,
                                             @RequestAttribute("correlationId") UUID correlationId) {
        return inventory.release(orderId, key, correlationId);
    }

    @GetMapping("/reservations/{orderId}/outbox")
    public List<OutboxPublisher.Delivery> outbox(@PathVariable UUID orderId) {
        transactions.reservation(orderId);
        return outbox.deliveries(orderId);
    }

    @GetMapping("/reservations/{orderId}/details")
    public InventoryDtos.Reservation details(@PathVariable UUID orderId, org.springframework.security.core.Authentication auth) {
        var result = transactions.reservation(orderId);
        boolean admin = auth.getAuthorities().stream().anyMatch(role -> role.getAuthority().equals("ROLE_ADMIN"));
        if (!admin && !result.customerId().toString().equals(auth.getName()))
            throw new com.synechisveltiosi.inventoryservice.application.ApiException(org.springframework.http.HttpStatus.NOT_FOUND, "RESERVATION_NOT_FOUND", "Reservation not found");
        return result;
    }
}
