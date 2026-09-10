package com.synechisveltiosi.orderqueryservice.infrastructure;

import com.synechisveltiosi.orderqueryservice.application.ProjectionTransactions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ProjectionListener {
    private final ProjectionCodec codec;
    private final ProjectionTransactions projection;

    public ProjectionListener(ProjectionCodec codec, ProjectionTransactions projection) {
        this.codec = codec;
        this.projection = projection;
    }

    @KafkaListener(topics = {"order.events", "inventory.events", "payment.events", "shipping.events", "notification.events"})
    public void receive(ConsumerRecord<String, String> record) {
        projection.accept(codec.sanitize(record.topic(), record.key(), record.value()));
    }
}
