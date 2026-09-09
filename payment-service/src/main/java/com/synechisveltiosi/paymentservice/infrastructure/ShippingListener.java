package com.synechisveltiosi.paymentservice.infrastructure;
import com.synechisveltiosi.paymentservice.application.RefundTransactions;
import com.synechisveltiosi.platform.contracts.EventEnvelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.core.type.TypeReference;
@Component
public class ShippingListener {
    private final JsonMapper json; private final RefundTransactions refunds;
    public ShippingListener(JsonMapper json, RefundTransactions refunds) { this.json = json; this.refunds = refunds; }
    @KafkaListener(topics = "shipping.events")
    public void receive(ConsumerRecord<String, String> record) {
        if (json.readTree(record.value()).required("eventType").asString().equals("ShipmentCreated")) return;
        var event = json.readValue(record.value(), new TypeReference<EventEnvelope<RefundTransactions.Input>>() { });
        if (!event.aggregateId().toString().equals(record.key())) throw new IllegalArgumentException("Wrong refund key");
        refunds.accept(event);
    }
}
