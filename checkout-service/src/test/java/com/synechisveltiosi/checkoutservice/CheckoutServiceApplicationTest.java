package com.synechisveltiosi.checkoutservice;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class CheckoutServiceApplicationTest {
    @Test
    void applicationStarts() {
        try (ConfigurableApplicationContext context = SpringApplication.run(CheckoutServiceApplication.class,
                "--spring.main.web-application-type=none")) {
            assertNotNull(context.getBean(CheckoutServiceApplication.class));
        }
    }
}
