package com.synechisveltiosi.cartservice;

import com.synechisveltiosi.cartservice.domain.Cart;
import org.junit.jupiter.api.Test;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class CartTest {
    @Test
    void addsSetsRemovesAndReturnsImmutableSnapshots() {
        Cart cart = new Cart(UUID.randomUUID());
        UUID product = UUID.randomUUID();
        cart.add(product, 2);
        var snapshot = cart.items();
        cart.add(product, 3);
        assertEquals(5, cart.items().get(product));
        assertEquals(2, snapshot.get(product));
        assertThrows(UnsupportedOperationException.class, () -> snapshot.clear());
        cart.set(product, 1);
        assertEquals(1, cart.items().get(product));
        cart.remove(product);
        assertTrue(cart.items().isEmpty());
    }

    @Test
    void rejectsExcessQuantityWithoutChangingExistingItems() {
        Cart cart = new Cart(UUID.randomUUID());
        UUID product = UUID.randomUUID();
        cart.add(product, 99);
        assertThrows(IllegalArgumentException.class, () -> cart.add(product, 1));
        assertThrows(IllegalArgumentException.class, () -> cart.set(product, 0));
        assertEquals(99, cart.items().get(product));
    }

    @Test
    void boundsDistinctItemsButAllowsUpdatingExistingLines() {
        Cart cart = new Cart(UUID.randomUUID());
        for (int i = 0; i < 50; i++) { cart.add(UUID.randomUUID(), 1); }
        assertThrows(IllegalArgumentException.class, () -> cart.add(UUID.randomUUID(), 1));
        cart.set(cart.items().keySet().iterator().next(), 2);
        assertEquals(50, cart.items().size());
    }
}
