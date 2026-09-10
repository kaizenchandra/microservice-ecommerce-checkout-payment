package com.synechisveltiosi.apigateway.infrastructure;
import io.opentelemetry.api.trace.*;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import static org.junit.jupiter.api.Assertions.*;
class TelemetryTest {
    @Test void restoresTraceAndLogContextAfterNestedWork() {
        var telemetry = new Telemetry("test", false, "http://127.0.0.1:1/v1/traces");
        String id = "12345678901234567890123456789012";
        var previous = Span.current().getSpanContext(); MDC.put("correlationId", "previous");
        try {
            try (var parent = telemetry.start("server", SpanKind.SERVER, "00-" + id + "-1234567890123456-01")) {
                String server = Span.current().getSpanContext().getSpanId();
                assertEquals(id, MDC.get("traceId"));
                try (var child = telemetry.start("child", SpanKind.INTERNAL, null)) { assertNotEquals(server, Span.current().getSpanContext().getSpanId()); }
                assertEquals(server, Span.current().getSpanContext().getSpanId());
            }
            assertEquals(previous, Span.current().getSpanContext()); assertNull(MDC.get("traceId")); assertEquals("previous", MDC.get("correlationId"));
        } finally { MDC.clear(); telemetry.close(); }
    }
}
