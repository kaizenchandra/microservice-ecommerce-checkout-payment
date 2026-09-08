package com.synechisveltiosi.inventoryservice;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class InventoryServiceApplicationTest {
    @Test
    void applicationStarts() {
        try (ConfigurableApplicationContext context = SpringApplication.run(InventoryServiceApplication.class,
                "--spring.main.web-application-type=none")) {
            assertNotNull(context.getBean(InventoryServiceApplication.class));
        }
    }
}
