package com.synechisveltiosi.inventoryservice;

import com.synechisveltiosi.inventoryservice.domain.Stock;
import com.synechisveltiosi.inventoryservice.domain.InventoryEvents;
import com.synechisveltiosi.inventoryservice.application.InventoryService;
import com.synechisveltiosi.inventoryservice.application.InventoryTransactions;
import org.junit.jupiter.api.Test;
import org.springframework.dao.OptimisticLockingFailureException;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class InventoryTest {
    @Test void stockProtectsAvailabilityAndReservedUnits() {
        var stock = new Stock(UUID.randomUUID(), 5);
        stock.reserve(3);
        assertEquals(2, stock.available());
        assertThrows(IllegalArgumentException.class, () -> stock.reserve(3));
        assertThrows(IllegalArgumentException.class, () -> stock.setOnHand(2));
        stock.release(3);
        assertEquals(5, stock.available());
        assertThrows(IllegalArgumentException.class, () -> stock.release(1));
        assertThrows(IllegalArgumentException.class, () -> new InventoryEvents.Line(UUID.randomUUID(), 0));
    }
    @Test void retriesThroughTransactionalBoundaryAndEventuallyPropagatesContention() {
        var transactions = mock(InventoryTransactions.class);
        var service = new InventoryService(transactions);
        doThrow(new OptimisticLockingFailureException("Concurrent write")).doNothing().when(transactions).reserve(null);
        service.reserve(null);
        verify(transactions, times(2)).reserve(null);
        reset(transactions);
        doThrow(new OptimisticLockingFailureException("Persistent contention")).when(transactions).reserve(null);
        assertThrows(OptimisticLockingFailureException.class, () -> service.reserve(null));
        verify(transactions, times(4)).reserve(null);
    }
}
