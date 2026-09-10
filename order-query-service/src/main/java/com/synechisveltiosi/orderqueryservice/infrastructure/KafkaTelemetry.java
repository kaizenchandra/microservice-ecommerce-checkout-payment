package com.synechisveltiosi.orderqueryservice.infrastructure;

import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.trace.SpanKind;
import org.apache.kafka.clients.consumer.*;
import org.slf4j.*;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;
import java.nio.charset.StandardCharsets;

@Component
public class KafkaTelemetry implements RecordInterceptor<Object, Object> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTelemetry.class);
    private final Telemetry telemetry; private final MeterRegistry metrics; private final JsonMapper json;
    private final ThreadLocal<Telemetry.Trace> active = new ThreadLocal<>();
    public KafkaTelemetry(Telemetry telemetry, MeterRegistry metrics, JsonMapper json) { this.telemetry = telemetry; this.metrics = metrics; this.json = json; }
    @Override public ConsumerRecord<Object, Object> intercept(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
        var header = record.headers().lastHeader("traceparent");
        String parent = header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
        if (parent == null) {
            try { var node = json.readTree((String) record.value()); var value = node.get("traceparent");
                if (value != null && !value.isNull()) parent = value.asString();
            } catch (RuntimeException ignored) { /* The listener will validate malformed records. */ }
        }
        var trace = telemetry.start("kafka.consume", SpanKind.CONSUMER, parent);
        active.set(trace); trace.attribute("messaging.destination.name", record.topic());
        try {
            var node = json.readTree((String) record.value());
            for (String key : new String[]{"correlationId", "eventId", "aggregateId"}) {
                if (node.get(key) != null) MDC.put(key.equals("aggregateId") ? "orderId" : key, java.util.UUID.fromString(node.get(key).asString()).toString());
            }
            String type = node.get("eventType").asString(); if (type.matches("[A-Za-z]{1,80}")) MDC.put("eventType", type);
        } catch (RuntimeException ignored) { /* Validation belongs to the listener; never log raw payloads. */ }
        return record;
    }
    @Override public void success(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
        metrics.counter("messaging.processed", "outcome", "success").increment(); LOG.info("Kafka record processed");
    }
    @Override public void failure(ConsumerRecord<Object, Object> record, Exception error, Consumer<Object, Object> consumer) {
        if (active.get() != null) active.get().failed(); metrics.counter("messaging.processed", "outcome", "failure").increment();
    }
    @Override public void afterRecord(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
        var trace = active.get(); active.remove(); if (trace != null) trace.close();
    }
}
