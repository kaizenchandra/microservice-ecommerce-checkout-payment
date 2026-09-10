package com.synechisveltiosi.notificationservice.infrastructure;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.context.*;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import jakarta.annotation.PreDestroy;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.time.Duration;
import java.util.*;

@Component
public class Telemetry {
    private final SdkTracerProvider provider;
    private final Tracer tracer;
    private static final TextMapGetter<Map<String, String>> GETTER = new TextMapGetter<>() {
        public Iterable<String> keys(Map<String, String> carrier) { return carrier.keySet(); }
        public String get(Map<String, String> carrier, String key) { return carrier.get(key); }
    };
    public Telemetry(@Value("${spring.application.name}") String service,
                     @Value("${telemetry.export-enabled:true}") boolean enabled,
                     @Value("${telemetry.endpoint:http://localhost:4318/v1/traces}") String endpoint) {
        var builder = SdkTracerProvider.builder().setResource(Resource.create(Attributes.builder().put("service.name", service).build()));
        if (enabled) builder.addSpanProcessor(BatchSpanProcessor.builder(OtlpHttpSpanExporter.builder()
                .setEndpoint(endpoint).setTimeout(Duration.ofSeconds(2)).build())
                .setMaxQueueSize(2048).setMaxExportBatchSize(128).setScheduleDelay(Duration.ofMillis(200)).build());
        provider = builder.build(); tracer = OpenTelemetrySdk.builder().setTracerProvider(provider).build().getTracer("checkout-platform");
    }
    public Trace start(String name, SpanKind kind, String parent) {
        Context context = parent == null ? Context.current() : W3CTraceContextPropagator.getInstance()
                .extract(Context.root(), Map.of("traceparent", parent), GETTER);
        return new Trace(tracer.spanBuilder(name).setParent(context).setSpanKind(kind).startSpan());
    }
    public static String currentTraceparentOr(String fallback) {
        var context = Span.current().getSpanContext();
        return context.isValid() ? "00-" + context.getTraceId() + "-" + context.getSpanId() + "-" + context.getTraceFlags().asHex() : fallback;
    }
    @PreDestroy void close() { provider.close(); }
    public static final class Trace implements AutoCloseable {
        private final Span span;
        private final Scope scope;
        private final Map<String, String> previous;
        private Trace(Span span) {
            this.span = span; previous = MDC.getCopyOfContextMap(); scope = span.makeCurrent();
            MDC.put("traceId", span.getSpanContext().getTraceId()); MDC.put("spanId", span.getSpanContext().getSpanId());
        }
        public void failed() { span.setStatus(StatusCode.ERROR); }
        public void attribute(String key, String value) { if (value != null) span.setAttribute(key, value); }
        public void close() { scope.close(); span.end(); if (previous == null) MDC.clear(); else MDC.setContextMap(previous); }
    }
}
