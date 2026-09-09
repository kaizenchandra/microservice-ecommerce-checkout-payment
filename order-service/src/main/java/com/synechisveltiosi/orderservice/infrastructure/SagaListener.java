package com.synechisveltiosi.orderservice.infrastructure;
import com.synechisveltiosi.orderservice.application.OrderSaga;
import com.synechisveltiosi.platform.contracts.EventEnvelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.core.type.TypeReference;
@Component
public class SagaListener {
    private final JsonMapper json; private final OrderSaga saga;
    public SagaListener(JsonMapper json, OrderSaga saga) { this.json = json; this.saga = saga; }
    @KafkaListener(topics = {"inventory.events", "payment.events", "shipping.events"})
    public void receive(ConsumerRecord<String, String> record) {
        var event = json.readValue(record.value(), new TypeReference<EventEnvelope<OrderSaga.Payload>>() { });
        String topic = switch (event.aggregateType()) { case "InventoryReservation" -> "inventory.events"; case "Payment" -> "payment.events"; case "Shipment" -> "shipping.events"; default -> throw new IllegalArgumentException("Unsupported saga producer"); };
        if (!topic.equals(record.topic()) || !event.aggregateId().toString().equals(record.key())) throw new IllegalArgumentException("Wrong saga topic or key");
        saga.accept(event);
    }
}
