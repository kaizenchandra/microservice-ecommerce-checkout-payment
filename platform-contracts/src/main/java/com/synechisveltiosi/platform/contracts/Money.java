package com.synechisveltiosi.platform.contracts;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Currency;
import java.util.Objects;

/** Nonnegative monetary value. Round explicitly at calculation boundaries, never on input. */
public record Money(BigDecimal amount, Currency currency) {
    public Money {
        Objects.requireNonNull(amount, "amount");
        Objects.requireNonNull(currency, "currency");
        int scale = currency.getDefaultFractionDigits();
        if (scale < 0) {
            throw new IllegalArgumentException("Currency must have defined minor units");
        }
        if (amount.signum() < 0) {
            throw new IllegalArgumentException("Amount cannot be negative");
        }
        amount = amount.setScale(scale, RoundingMode.UNNECESSARY);
    }

    public static Money rounded(BigDecimal amount, Currency currency) {
        Objects.requireNonNull(currency, "currency");
        if (currency.getDefaultFractionDigits() < 0 || amount.signum() < 0) {
            throw new IllegalArgumentException("Invalid monetary amount or currency");
        }
        return new Money(amount.setScale(currency.getDefaultFractionDigits(), RoundingMode.HALF_EVEN), currency);
    }

    public Money add(Money other) {
        if (!currency.equals(other.currency)) {
            throw new IllegalArgumentException("Currency mismatch");
        }
        return new Money(amount.add(other.amount), currency);
    }

    public Money multiply(int quantity) {
        if (quantity < 0) {
            throw new IllegalArgumentException("Quantity cannot be negative");
        }
        return new Money(amount.multiply(BigDecimal.valueOf(quantity)), currency);
    }
}
