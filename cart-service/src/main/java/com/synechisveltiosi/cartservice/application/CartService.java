package com.synechisveltiosi.cartservice.application;

import com.synechisveltiosi.cartservice.api.CartDtos;
import com.synechisveltiosi.cartservice.infrastructure.ProductCatalogClient;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class CartService {
    private final CartTransactions transactions;
    private final ProductCatalogClient catalog;

    public CartService(CartTransactions transactions, ProductCatalogClient catalog) {
        this.transactions = transactions;
        this.catalog = catalog;
    }

    public CartDtos.View change(UUID cart, UUID customer, UUID product, int quantity, long version,
                                CartTransactions.Mutation mutation, String authorization) {
        // Verify ownership before fan-out; no DB transaction spans the HTTP call.
        transactions.get(cart, customer);
        if (mutation != CartTransactions.Mutation.REMOVE) {
            catalog.requireActive(product, authorization);
        }
        // Reload and compare version after the HTTP response: concurrent writes may have happened.
        return transactions.change(cart, customer, product, quantity, version, mutation);
    }
}
