package com.synechisveltiosi.platform.contracts;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Currency;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MoneyTest {
    private static final Currency USD = Currency.getInstance("USD");

    @Test
    void normalizesScaleAndComputesExactly() {
        Money unit = new Money(new BigDecimal("19.90"), USD);
        assertEquals(new Money(new BigDecimal("59.70"), USD), unit.multiply(3));
        assertEquals(new Money(new BigDecimal("20"), USD), unit.add(new Money(new BigDecimal("0.10"), USD)));
    }

    @Test
    void rejectsSilentRoundingNegativeAmountsAndMixedCurrencies() {
        assertThrows(ArithmeticException.class, () -> new Money(new BigDecimal("1.001"), USD));
        assertThrows(IllegalArgumentException.class, () -> new Money(new BigDecimal("-1"), USD));
        Money dollar = new Money(BigDecimal.ONE, USD);
        assertThrows(IllegalArgumentException.class, () -> dollar.multiply(-1));
        assertThrows(IllegalArgumentException.class, () -> dollar.add(new Money(BigDecimal.ONE, Currency.getInstance("EUR"))));
    }

    @Test
    void roundsOnlyExplicitlyUsingHalfEvenAndCurrencyMinorUnits() {
        assertEquals(new BigDecimal("1.00"), Money.rounded(new BigDecimal("1.005"), USD).amount());
        assertEquals(new BigDecimal("1.02"), Money.rounded(new BigDecimal("1.015"), USD).amount());
        assertEquals(new BigDecimal("2"), Money.rounded(new BigDecimal("1.5"), Currency.getInstance("JPY")).amount());
        assertThrows(IllegalArgumentException.class, () -> Money.rounded(new BigDecimal("-0.001"), USD));
        assertThrows(IllegalArgumentException.class, () -> new Money(BigDecimal.ZERO, Currency.getInstance("XXX")));
    }
}
