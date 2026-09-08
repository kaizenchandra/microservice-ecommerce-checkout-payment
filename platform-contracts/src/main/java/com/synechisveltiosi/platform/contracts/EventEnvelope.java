package com.synechisveltiosi.platform.contracts;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Transport metadata only: payload types and event names belong to their producing service. */
public record EventEnvelope<T>(
        UUID eventId,
        String eventType,
        UUID correlationId,
        UUID causationId,
        String aggregateType,
        UUID aggregateId,
        long aggregateVersion,
        Instant occurredAt,
        int schemaVersion,
        String traceparent,
        T payload
) {
    public EventEnvelope {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(correlationId, "correlationId");
        Objects.requireNonNull(causationId, "causationId");
        Objects.requireNonNull(aggregateId, "aggregateId");
        Objects.requireNonNull(occurredAt, "occurredAt");
        Objects.requireNonNull(payload, "payload");
        if (eventType == null || eventType.isBlank() || aggregateType == null || aggregateType.isBlank()) {
            throw new IllegalArgumentException("Event and aggregate types are required");
        }
        if (aggregateVersion < 1 || schemaVersion < 1) {
            throw new IllegalArgumentException("Versions start at one");
        }
        if (traceparent != null && !traceparent.matches("00-(?!0{32}-)[0-9a-f]{32}-(?!0{16}-)[0-9a-f]{16}-[0-9a-f]{2}")) {
            throw new IllegalArgumentException("Expected W3C version 00 traceparent");
        }
    }
}
