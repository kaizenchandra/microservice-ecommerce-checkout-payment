package com.synechisveltiosi.shippingservice;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class ShippingServiceApplicationTest {
    @Test
    void applicationStarts() {
        try (ConfigurableApplicationContext context = SpringApplication.run(ShippingServiceApplication.class,
                "--spring.main.web-application-type=none")) {
            assertNotNull(context.getBean(ShippingServiceApplication.class));
        }
    }
}
