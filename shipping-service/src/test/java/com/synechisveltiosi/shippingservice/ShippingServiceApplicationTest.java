package com.synechisveltiosi.shippingservice;
import com.synechisveltiosi.shippingservice.domain.ShippingEvents;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class ShippingServiceApplicationTest {
    @Test void requiresBoundedSyntheticAddress() {
        assertThrows(IllegalArgumentException.class, () -> new ShippingEvents.Address("", "Street", "City", "12345", "US"));
        assertEquals("ZZ", new ShippingEvents.Address("Demo", "Street", "City", "12345", "ZZ").country());
    }
}
