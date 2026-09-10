package com.synechisveltiosi.inventoryservice.infrastructure;

import com.synechisveltiosi.inventoryservice.application.InventoryService;
import com.synechisveltiosi.inventoryservice.domain.InventoryEvents;
import com.synechisveltiosi.platform.contracts.EventEnvelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
public class OrderEventListener {
    private final JsonMapper json;
    private final InventoryService inventory;

    public OrderEventListener(JsonMapper json, InventoryService inventory) {
        this.json = json;
        this.inventory = inventory;
    }

    @KafkaListener(topics = "order.events")
    public void receive(ConsumerRecord<String, String> record) {
        var node = json.readTree(record.value());
        String type = node.required("eventType").asString();
        // Known order facts that do not reserve stock. Future types must be explicitly classified.
        if (java.util.Set.of("OrderNoteAdded", "OrderFactRecorded", "OrderCompleted", "OrderCancelled").contains(type))
            return;
        if (!"OrderCreated".equals(type)) throw new IllegalArgumentException("Unsupported order event type");
        var event = json.readValue(record.value(),
                new tools.jackson.core.type.TypeReference<EventEnvelope<InventoryEvents.OrderCreated>>() {
                });
        if (!event.payload().orderId().toString().equals(record.key()))
            throw new IllegalArgumentException("Order event key mismatch");
        inventory.reserve(event);
    }
}
