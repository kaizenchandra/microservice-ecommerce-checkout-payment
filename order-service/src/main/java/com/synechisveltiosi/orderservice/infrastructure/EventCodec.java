package com.synechisveltiosi.orderservice.infrastructure;

import com.synechisveltiosi.orderservice.domain.OrderEvents;
import com.synechisveltiosi.platform.contracts.EventEnvelope;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.UUID;

@Component
public class EventCodec {
    private final JsonMapper mapper;
    public EventCodec(JsonMapper mapper) { this.mapper = mapper; }
    public String encode(Object value) { return mapper.writeValueAsString(value); }
    public <T> T read(String json, Class<T> type) { return mapper.readValue(json, type); }

    public String type(OrderEvents.Event event) {
        return switch (event) {
            case OrderEvents.OrderCreated ignored -> "OrderCreated";
            case OrderEvents.OrderNoteAdded ignored -> "OrderNoteAdded";
            case OrderEvents.OrderFactRecorded ignored -> "OrderFactRecorded";
            case OrderEvents.OrderCompleted ignored -> "OrderCompleted";
            case OrderEvents.OrderCancelled ignored -> "OrderCancelled";
        };
    }

    public EventEnvelope<OrderEvents.Event> decode(String json) {
        JsonNode node = mapper.readTree(json);
        if (node.get("schemaVersion").asInt() != 1 || !"Order".equals(node.get("aggregateType").asString())) {
            throw new IllegalStateException("Unsupported order event schema or aggregate type");
        }
        String type = node.get("eventType").asString();
        OrderEvents.Event event = switch (type) {
            case "OrderCreated" -> mapper.treeToValue(node.get("payload"), OrderEvents.OrderCreated.class);
            case "OrderNoteAdded" -> mapper.treeToValue(node.get("payload"), OrderEvents.OrderNoteAdded.class);
            case "OrderFactRecorded" -> mapper.treeToValue(node.get("payload"), OrderEvents.OrderFactRecorded.class);
            case "OrderCompleted" -> mapper.treeToValue(node.get("payload"), OrderEvents.OrderCompleted.class);
            case "OrderCancelled" -> mapper.treeToValue(node.get("payload"), OrderEvents.OrderCancelled.class);
            default -> throw new IllegalStateException("Unsupported order event type");
        };
        JsonNode trace = node.get("traceparent");
        return new EventEnvelope<>(UUID.fromString(node.get("eventId").asString()), type,
                UUID.fromString(node.get("correlationId").asString()), UUID.fromString(node.get("causationId").asString()),
                "Order", UUID.fromString(node.get("aggregateId").asString()), node.get("aggregateVersion").asLong(),
                Instant.parse(node.get("occurredAt").asString()), 1,
                trace == null || trace.isNull() ? null : trace.asString(), event);
    }
}
