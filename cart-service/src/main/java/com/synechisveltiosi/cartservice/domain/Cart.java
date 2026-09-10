package com.synechisveltiosi.cartservice.domain;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@Entity
@Table(name = "cart")
public class Cart {
    public static final int MAX_ITEMS = 50;
    public static final int MAX_QUANTITY = 99;
    @Id
    private UUID id;
    @Column(nullable = false, updatable = false)
    private UUID customerId;
    @Version
    private long version;
    @Column(nullable = false, updatable = false)
    private Instant createdAt;
    @Column(nullable = false)
    private Instant updatedAt;
    @ElementCollection
    @CollectionTable(name = "cart_item", joinColumns = @JoinColumn(name = "cart_id"))
    @MapKeyColumn(name = "product_id")
    @Column(name = "quantity", nullable = false)
    private Map<UUID, Integer> items = new HashMap<>();

    protected Cart() {
    }

    public Cart(UUID customerId) {
        this.id = UUID.randomUUID();
        this.customerId = Objects.requireNonNull(customerId);
        this.createdAt = Instant.now();
        this.updatedAt = createdAt;
    }

    private static void requireQuantity(int quantity) {
        if (quantity < 1 || quantity > MAX_QUANTITY) {
            throw new IllegalArgumentException("Quantity must be between 1 and 99");
        }
    }

    public void add(UUID productId, int quantity) {
        requireQuantity(quantity);
        set(productId, Math.addExact(items.getOrDefault(productId, 0), quantity));
    }

    public void set(UUID productId, int quantity) {
        Objects.requireNonNull(productId);
        requireQuantity(quantity);
        if (!items.containsKey(productId) && items.size() >= MAX_ITEMS) {
            throw new IllegalArgumentException("A cart may contain at most 50 distinct products");
        }
        items.put(productId, quantity);
        updatedAt = Instant.now();
    }

    public void remove(UUID productId) {
        items.remove(Objects.requireNonNull(productId));
        updatedAt = Instant.now();
    }

    public UUID id() {
        return id;
    }

    public UUID customerId() {
        return customerId;
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

    public Map<UUID, Integer> items() {
        return Map.copyOf(items);
    }
}
