package com.synechisveltiosi.shippingservice.infrastructure;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import java.util.Map;

@Configuration
public class KafkaConfiguration {
    /** Never acknowledge failed business work. Operational DLT/redrive is introduced in Phase 10. */
    @Bean
    DefaultErrorHandler shippingErrorHandler() {
        var handler = new DefaultErrorHandler((record, exception) -> {
            throw new IllegalStateException("Inventory event requires operator recovery");
        }, new FixedBackOff(1000L, FixedBackOff.UNLIMITED_ATTEMPTS));
        handler.setClassifications(Map.of(Exception.class, true), true);
        return handler;
    }
}
