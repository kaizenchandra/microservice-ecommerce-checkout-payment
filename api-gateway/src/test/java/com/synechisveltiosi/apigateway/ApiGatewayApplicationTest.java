package com.synechisveltiosi.apigateway;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class ApiGatewayApplicationTest {
    @Test
    void applicationStarts() {
        try (ConfigurableApplicationContext context = SpringApplication.run(ApiGatewayApplication.class,
                "--spring.main.web-application-type=none")) {
            assertNotNull(context.getBean(ApiGatewayApplication.class));
        }
    }
}
