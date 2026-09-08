package com.synechisveltiosi.cartservice.application;

import com.synechisveltiosi.cartservice.api.CartDtos;
import com.synechisveltiosi.cartservice.domain.Cart;
import com.synechisveltiosi.cartservice.infrastructure.CartRepository;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.UUID;

@Service
public class CartTransactions {
    public enum Mutation { ADD, SET, REMOVE }
    private final CartRepository carts;
    public CartTransactions(CartRepository carts) { this.carts = carts; }

    @Transactional
    public CartDtos.View create(UUID customer) {
        return CartDtos.View.from(carts.saveAndFlush(new Cart(customer)));
    }

    @Transactional(readOnly = true)
    public CartDtos.View get(UUID id, UUID customer) { return CartDtos.View.from(find(id, customer)); }

    @Transactional
    public CartDtos.View change(UUID id, UUID customer, UUID product, int quantity, long expectedVersion, Mutation mutation) {
        Cart cart = find(id, customer);
        if (cart.version() != expectedVersion) {
            throw new ApiException(HttpStatus.CONFLICT, "STALE_VERSION", "Cart changed; reload before updating");
        }
        try {
            switch (mutation) {
                case ADD -> cart.add(product, quantity);
                case SET -> cart.set(product, quantity);
                case REMOVE -> cart.remove(product);
            }
        } catch (IllegalArgumentException error) {
            throw new ApiException(HttpStatus.CONFLICT, "CART_LIMIT_EXCEEDED", error.getMessage());
        }
        carts.flush();
        return CartDtos.View.from(cart);
    }

    private Cart find(UUID id, UUID customer) {
        return carts.findByIdAndCustomerId(id, customer)
                .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "CART_NOT_FOUND", "Cart not found"));
    }
}
