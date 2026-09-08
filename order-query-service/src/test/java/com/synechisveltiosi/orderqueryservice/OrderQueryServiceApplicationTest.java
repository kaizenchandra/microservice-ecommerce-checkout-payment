package com.synechisveltiosi.orderqueryservice;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class OrderQueryServiceApplicationTest {
    @Test
    void applicationStarts() {
        try (ConfigurableApplicationContext context = SpringApplication.run(OrderQueryServiceApplication.class,
                "--spring.main.web-application-type=none")) {
            assertNotNull(context.getBean(OrderQueryServiceApplication.class));
        }
    }
}
