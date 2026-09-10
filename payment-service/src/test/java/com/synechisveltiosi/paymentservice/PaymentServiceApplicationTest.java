package com.synechisveltiosi.paymentservice;

import com.synechisveltiosi.paymentservice.application.PaymentProvider;
import com.synechisveltiosi.paymentservice.domain.ChargeResult;
import com.synechisveltiosi.platform.contracts.Money;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Currency;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PaymentServiceApplicationTest {
    @Test
    void onlyTerminalProviderResultsCanFinalizePayments() {
        assertThrows(IllegalArgumentException.class, () -> new ChargeResult("UNKNOWN", null));
        assertThrows(IllegalArgumentException.class, () -> new ChargeResult("COMPLETED", null));
        assertEquals("FAILED", new ChargeResult("FAILED", null).status());
    }

    @Test
    void providerAcceptsOnlyBoundedDemoInputs() {
        var amount = new Money(new BigDecimal("25.00"), Currency.getInstance("USD"));
        assertThrows(IllegalArgumentException.class, () -> new PaymentProvider.Request(UUID.randomUUID(), amount, "not-a-demo-token"));
        assertThrows(IllegalArgumentException.class, () -> new PaymentProvider.Request(UUID.randomUUID(), new Money(BigDecimal.ZERO, Currency.getInstance("USD")), "tok_success"));
        assertEquals(amount, new PaymentProvider.Request(UUID.randomUUID(), amount, "tok_timeout").total());
    }
}
