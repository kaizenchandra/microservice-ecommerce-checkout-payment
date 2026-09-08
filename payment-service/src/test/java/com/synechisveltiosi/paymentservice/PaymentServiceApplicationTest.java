package com.synechisveltiosi.paymentservice;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class PaymentServiceApplicationTest {
    @Test
    void applicationStarts() {
        try (ConfigurableApplicationContext context = SpringApplication.run(PaymentServiceApplication.class,
                "--spring.main.web-application-type=none")) {
            assertNotNull(context.getBean(PaymentServiceApplication.class));
        }
    }
}
