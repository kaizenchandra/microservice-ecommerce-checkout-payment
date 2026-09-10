package com.synechisveltiosi.notificationservice.infrastructure;

import com.synechisveltiosi.notificationservice.application.NotificationTransactions;
import com.synechisveltiosi.platform.contracts.EventEnvelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

@Component
public class OrderListener {
    private final JsonMapper json;
    private final NotificationTransactions notifications;

    public OrderListener(JsonMapper json, NotificationTransactions notifications) {
        this.json = json;
        this.notifications = notifications;
    }

    @KafkaListener(topics = "order.events")
    public void receive(ConsumerRecord<String, String> record) {
        String type = json.readTree(record.value()).required("eventType").asString();
        if (java.util.Set.of("OrderCreated", "OrderNoteAdded", "OrderFactRecorded").contains(type)) return;
        var event = json.readValue(record.value(), new TypeReference<EventEnvelope<NotificationTransactions.Payload>>() {
        });
        if (!event.aggregateId().toString().equals(record.key()))
            throw new IllegalArgumentException("Wrong notification key");
        notifications.accept(event);
    }
}
