package com.synechisveltiosi.inventoryservice.application;

import com.synechisveltiosi.inventoryservice.api.InventoryDtos;
import com.synechisveltiosi.inventoryservice.domain.InventoryEvents;
import com.synechisveltiosi.platform.contracts.EventEnvelope;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.stereotype.Service;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

/** Retry outside the transactional proxy so every attempt loads fresh stock and inbox state. */
@Service
public class InventoryService {
    private final InventoryTransactions transactions;
    public InventoryService(InventoryTransactions transactions) { this.transactions = transactions; }
    public void reserve(EventEnvelope<InventoryEvents.OrderCreated> event) {
        retry(() -> { transactions.reserve(event); return null; });
    }
    public InventoryDtos.Reservation release(UUID orderId, UUID commandId, UUID correlationId) {
        return retry(() -> transactions.release(orderId, commandId, correlationId));
    }
    private <T> T retry(Supplier<T> action) {
        for (int attempt = 0; ; attempt++) {
            try { return action.get(); }
            catch (OptimisticLockingFailureException | CannotAcquireLockException conflict) {
                if (attempt == 3) throw conflict;
                try { Thread.sleep((20L << attempt) + ThreadLocalRandom.current().nextLong(20)); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException("Inventory retry interrupted", e); }
            }
        }
    }
}
