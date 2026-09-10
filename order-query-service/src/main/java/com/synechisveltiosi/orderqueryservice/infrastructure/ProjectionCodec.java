package com.synechisveltiosi.orderqueryservice.infrastructure;

import com.synechisveltiosi.orderqueryservice.domain.QueryEvents;
import com.synechisveltiosi.platform.contracts.EventEnvelope;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;

@Component
public class ProjectionCodec {
    private final JsonMapper json;

    public ProjectionCodec(JsonMapper json) {
        this.json = json;
    }

    public EventEnvelope<JsonNode> decode(String value) {
        return json.readValue(value, new TypeReference<EventEnvelope<JsonNode>>() {
        });
    }

    public EventEnvelope<JsonNode> sanitize(String topic, String key, String value) {
        var event = decode(value);
        if (event.schemaVersion() != 1 || !event.aggregateId().toString().equals(key))
            throw new IllegalArgumentException("Invalid projection schema or key");
        String producer;
        Class<?> type;
        long version = 0;
        switch (event.eventType()) {
            case "OrderCreated" -> {
                producer = "Order";
                type = QueryEvents.Created.class;
                version = 1;
            }
            case "OrderNoteAdded" -> {
                producer = "Order";
                type = QueryEvents.Note.class;
            }
            case "OrderFactRecorded" -> {
                producer = "Order";
                type = QueryEvents.Recorded.class;
            }
            case "OrderCompleted", "OrderCancelled" -> {
                producer = "Order";
                type = QueryEvents.Terminal.class;
            }
            case "InventoryReserved", "InventoryReservationFailed" -> {
                producer = "InventoryReservation";
                type = QueryEvents.Inventory.class;
                version = 1;
            }
            case "InventoryReleased" -> {
                producer = "InventoryReservation";
                type = QueryEvents.Inventory.class;
                version = 2;
            }
            case "PaymentCompleted", "PaymentFailed" -> {
                producer = "Payment";
                type = QueryEvents.Payment.class;
                version = 1;
            }
            case "PaymentRefunded" -> {
                producer = "Payment";
                type = QueryEvents.Payment.class;
                version = 2;
            }
            case "ShipmentCreated", "ShipmentFailed" -> {
                producer = "Shipment";
                type = QueryEvents.Shipment.class;
                version = 1;
            }
            case "CustomerNotified" -> {
                producer = "Notification";
                type = QueryEvents.Notification.class;
                version = 1;
            }
            default -> throw new IllegalArgumentException("Unsupported projection event");
        }
        String expectedTopic = switch (producer) {
            case "Order" -> "order.events";
            case "InventoryReservation" -> "inventory.events";
            case "Payment" -> "payment.events";
            case "Shipment" -> "shipping.events";
            default -> "notification.events";
        };
        if (!producer.equals(event.aggregateType()) || !expectedTopic.equals(topic) || (version != 0 && event.aggregateVersion() != version)
                || (version == 0 && event.aggregateVersion() < 2))
            throw new IllegalArgumentException("Wrong projection producer or version");
        JsonNode payload = json.valueToTree(json.treeToValue(event.payload(), type));
        if (payload.has("orderId") && !event.aggregateId().toString().equals(payload.get("orderId").asString()))
            throw new IllegalArgumentException("Wrong payload order");
        if (event.eventType().equals("PaymentCompleted") && payload.get("providerReference").isNull())
            throw new IllegalArgumentException("Missing charge reference");
        if (event.eventType().equals("PaymentRefunded") && payload.get("refundReference").isNull())
            throw new IllegalArgumentException("Missing refund reference");
        if (event.eventType().equals("ShipmentCreated") && (payload.get("trackingNumber").isNull() || payload.get("trackingNumber").asString().isBlank()))
            throw new IllegalArgumentException("Missing tracking reference");
        return new EventEnvelope<>(event.eventId(), event.eventType(), event.correlationId(), event.causationId(), producer, event.aggregateId(), event.aggregateVersion(),
                event.occurredAt(), 1, event.traceparent(), payload);
    }

    public String encode(Object value) {
        return json.writeValueAsString(value);
    }

    public <T> T read(String value, Class<T> type) {
        return json.readValue(value, type);
    }

    public <T> T payload(EventEnvelope<JsonNode> event, Class<T> type) {
        return json.treeToValue(event.payload(), type);
    }

    public String hash(EventEnvelope<JsonNode> event) {
        String value = event.aggregateType() + ":" + event.aggregateId() + ":" + event.aggregateVersion() + ":" + event.eventType() + ":" + encode(event.payload());
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
