package com.synechisveltiosi.orderqueryservice;

import com.synechisveltiosi.orderqueryservice.domain.QueryEvents;
import com.synechisveltiosi.platform.contracts.Money;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Currency;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OrderQueryServiceApplicationTest {
    @Test
    void validatesReadModelSnapshotAndDefaultsOldSalesChannel() {
        var line = new QueryEvents.Line(UUID.randomUUID(), "SKU-1", "Demo", 2, new Money(new BigDecimal("12.50"), Currency.getInstance("USD")));
        var address = new QueryEvents.Address("Demo", "Street", "City", "12345", "US");
        var total = new Money(new BigDecimal("25"), Currency.getInstance("USD"));
        assertEquals("WEB", new QueryEvents.Created(UUID.randomUUID(), UUID.randomUUID(), List.of(line), total, address, null).salesChannel());
        assertThrows(IllegalArgumentException.class, () -> new QueryEvents.Created(UUID.randomUUID(), UUID.randomUUID(), List.of(line), line.unitPrice(), address, null));
    }
}
