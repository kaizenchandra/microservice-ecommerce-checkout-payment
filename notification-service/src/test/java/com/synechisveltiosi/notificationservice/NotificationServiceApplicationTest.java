package com.synechisveltiosi.notificationservice;
import com.synechisveltiosi.notificationservice.application.NotificationTransactions;
import org.junit.jupiter.api.Test;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;
class NotificationServiceApplicationTest {
    @Test void notificationRequiresOrderAndCustomerIdentity() {
        assertThrows(NullPointerException.class, () -> new NotificationTransactions.Payload(UUID.randomUUID(), null));
    }
}
