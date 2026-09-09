package com.synechisveltiosi.inventoryservice.infrastructure;
import com.synechisveltiosi.inventoryservice.application.CompensationTransactions;
import com.synechisveltiosi.platform.contracts.EventEnvelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.core.type.TypeReference;
@Component
public class CompensationListener {
    private final JsonMapper json; private final CompensationTransactions compensation;
    public CompensationListener(JsonMapper json, CompensationTransactions compensation) { this.json = json; this.compensation = compensation; }
    @KafkaListener(topics = "payment.events")
    public void receive(ConsumerRecord<String, String> record) {
        String type = json.readTree(record.value()).required("eventType").asString();
        if (type.equals("PaymentCompleted")) return;
        var event = json.readValue(record.value(), new TypeReference<EventEnvelope<CompensationTransactions.Payload>>() { });
        if (!event.aggregateId().toString().equals(record.key())) throw new IllegalArgumentException("Wrong compensation key");
        compensation.accept(event);
    }
}
