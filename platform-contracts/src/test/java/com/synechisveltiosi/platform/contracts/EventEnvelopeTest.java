package com.synechisveltiosi.platform.contracts;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class EventEnvelopeTest {
    private EventEnvelope<String> envelope(long version, int schema, String trace) {
        return new EventEnvelope<>(UUID.randomUUID(), "OrderCreated", UUID.randomUUID(), UUID.randomUUID(),
                "Order", UUID.randomUUID(), version, Instant.now(), schema, trace, "payload");
    }

    @Test
    void acceptsUntracedEventsAndValidTraceContext() {
        assertNull(envelope(1, 1, null).traceparent());
        assertNotNull(envelope(2, 1, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"));
    }

    @Test
    void rejectsInvalidVersionsAndTraceContext() {
        assertThrows(IllegalArgumentException.class, () -> envelope(0, 1, null));
        assertThrows(IllegalArgumentException.class, () -> envelope(1, 0, null));
        assertThrows(IllegalArgumentException.class, () -> envelope(1, 1, "not-a-trace"));
        assertThrows(IllegalArgumentException.class, () -> envelope(1, 1, "00-00000000000000000000000000000000-00f067aa0ba902b7-01"));
    }
}
