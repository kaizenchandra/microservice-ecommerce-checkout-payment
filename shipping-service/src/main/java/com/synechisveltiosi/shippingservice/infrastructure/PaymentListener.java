package com.synechisveltiosi.shippingservice.infrastructure;

import com.synechisveltiosi.shippingservice.application.ShippingTransactions;
import com.synechisveltiosi.shippingservice.domain.ShippingEvents;
import com.synechisveltiosi.platform.contracts.EventEnvelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.core.type.TypeReference;

@Component
public class PaymentListener {
    private final JsonMapper json; private final ShippingTransactions shipping;
    public PaymentListener(JsonMapper json, ShippingTransactions shipping) { this.json = json; this.shipping = shipping; }
    @KafkaListener(topics = "payment.events")
    public void receive(ConsumerRecord<String, String> record) {
        String type = json.readTree(record.value()).required("eventType").asString();
        if (java.util.Set.of("PaymentFailed", "PaymentRefunded").contains(type)) return;
        if (!type.equals("PaymentCompleted")) throw new IllegalArgumentException("Unsupported payment event");
        var event = json.readValue(record.value(), new TypeReference<EventEnvelope<ShippingEvents.PaymentCompleted>>() { });
        if (!event.aggregateId().toString().equals(record.key())) throw new IllegalArgumentException("Wrong order key");
        shipping.accept(event);
    }
}
