package com.synechisveltiosi.paymentservice.infrastructure;

import com.synechisveltiosi.paymentservice.application.PaymentTransactions;
import com.synechisveltiosi.paymentservice.domain.PaymentEvents;
import com.synechisveltiosi.platform.contracts.EventEnvelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import java.util.Set;

@Component
public class InventoryEventListener {
    private final JsonMapper json;
    private final PaymentTransactions payments;

    public InventoryEventListener(JsonMapper json, PaymentTransactions payments) {
        this.json = json;
        this.payments = payments;
    }

    @KafkaListener(topics = "inventory.events")
    public void receive(ConsumerRecord<String, String> record) {
        String type = json.readTree(record.value()).required("eventType").asString();
        if (Set.of("InventoryReservationFailed", "InventoryReleased").contains(type)) return;
        if (!"InventoryReserved".equals(type)) throw new IllegalArgumentException("Unsupported inventory event type");
        var event = json.readValue(record.value(), new TypeReference<EventEnvelope<PaymentEvents.InventoryReserved>>() {
        });
        if (!event.payload().orderId().toString().equals(record.key()))
            throw new IllegalArgumentException("Inventory event key mismatch");
        payments.accept(event);
    }
}
