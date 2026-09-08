package com.synechisveltiosi.inventoryservice.domain;

import jakarta.persistence.*;
import java.util.UUID;

/** onHand includes reserved units; availability is derived, never independently written. */
@Entity
@Table(name = "stock")
public class Stock {
    @Id private UUID productId;
    @Column(nullable = false) private int onHand;
    @Column(nullable = false) private int reserved;
    @Version private long version;
    protected Stock() { }
    public Stock(UUID productId, int onHand) {
        this.productId = java.util.Objects.requireNonNull(productId);
        setOnHand(onHand);
    }
    public void setOnHand(int quantity) {
        if (quantity < reserved || quantity < 0 || quantity > 1000000) throw new IllegalArgumentException("Stock must be between reserved quantity and 1000000");
        onHand = quantity;
    }
    public void reserve(int quantity) {
        if (quantity < 1 || quantity > available()) throw new IllegalArgumentException("Insufficient stock");
        reserved += quantity;
    }
    public void release(int quantity) {
        if (quantity < 1 || quantity > reserved) throw new IllegalArgumentException("Invalid release quantity");
        reserved -= quantity;
    }
    public UUID productId() { return productId; }
    public int onHand() { return onHand; }
    public int reserved() { return reserved; }
    public int available() { return onHand - reserved; }
    public long version() { return version; }
}
