package com.synechisveltiosi.productservice.domain;

import com.synechisveltiosi.platform.contracts.Money;
import jakarta.persistence.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Currency;
import java.util.UUID;

@Entity
@Table(name = "product")
public class Product {
    @Id
    private UUID id;
    @Column(nullable = false, unique = true, length = 40)
    private String sku;
    @Column(nullable = false, length = 160)
    private String name;
    @Column(nullable = false, length = 2000)
    private String description;
    @Column(nullable = false, precision = 14, scale = 2)
    private BigDecimal amount;
    @Column(nullable = false, length = 3)
    private String currency;
    @Column(nullable = false)
    private boolean active;
    @Version
    private long version;
    @Column(nullable = false, updatable = false)
    private Instant createdAt;
    @Column(nullable = false)
    private Instant updatedAt;

    protected Product() {
    }

    public Product(String sku, String name, String description, Money price) {
        if (sku == null || !sku.matches("[A-Z0-9][A-Z0-9-]{1,39}")) {
            throw new IllegalArgumentException("Invalid SKU");
        }
        this.id = UUID.randomUUID();
        this.sku = sku;
        this.createdAt = Instant.now();
        revise(name, description, price, true);
    }

    public void revise(String name, String description, Money price, boolean active) {
        if (name == null || name.isBlank() || name.length() > 160 || description == null || description.length() > 2000) {
            throw new IllegalArgumentException("Invalid product description");
        }
        if (!price.currency().equals(Currency.getInstance("USD")) || price.amount().signum() <= 0
                || price.amount().compareTo(new BigDecimal("999999999999.99")) > 0) {
            throw new IllegalArgumentException("The demo supports positive USD prices within twelve integer digits");
        }
        this.name = name.strip();
        this.description = description.strip();
        this.amount = price.amount();
        this.currency = price.currency().getCurrencyCode();
        this.active = active;
        this.updatedAt = Instant.now();
    }

    public UUID id() {
        return id;
    }

    public String sku() {
        return sku;
    }

    public String name() {
        return name;
    }

    public String description() {
        return description;
    }

    public Money price() {
        return new Money(amount, Currency.getInstance(currency));
    }

    public boolean active() {
        return active;
    }

    public long version() {
        return version;
    }

    public Instant createdAt() {
        return createdAt;
    }

    public Instant updatedAt() {
        return updatedAt;
    }
}
