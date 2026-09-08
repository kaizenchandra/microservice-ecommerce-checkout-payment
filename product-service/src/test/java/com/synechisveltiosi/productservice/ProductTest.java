package com.synechisveltiosi.productservice;

import com.synechisveltiosi.platform.contracts.Money;
import com.synechisveltiosi.productservice.domain.Product;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.util.Currency;
import static org.junit.jupiter.api.Assertions.*;

class ProductTest {
    private Money usd(String amount) { return new Money(new BigDecimal(amount), Currency.getInstance("USD")); }

    @Test
    void normalizesNamesAndPreservesExactPrices() {
        Product product = new Product("SKU-1", " Keyboard ", " Example ", usd("79.90"));
        assertEquals("Keyboard", product.name());
        assertEquals(usd("79.90"), product.price());
        product.revise("Keyboard", "Updated", usd("89.90"), false);
        assertFalse(product.active());
        assertEquals(usd("89.90"), product.price());
    }

    @Test
    void rejectsInvalidCatalogState() {
        assertThrows(IllegalArgumentException.class, () -> new Product("bad sku", "Name", "", usd("1")));
        assertThrows(IllegalArgumentException.class, () -> new Product("SKU-1", " ", "", usd("1")));
        assertThrows(IllegalArgumentException.class, () -> new Product("SKU-1", "Name", "", usd("0")));
        assertThrows(IllegalArgumentException.class, () -> new Product("SKU-1", "Name", "",
                new Money(BigDecimal.ONE, Currency.getInstance("EUR"))));
    }
}
