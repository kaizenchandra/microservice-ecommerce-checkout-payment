package com.synechisveltiosi.apigateway.infrastructure;

import io.opentelemetry.api.trace.SpanKind;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.*;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CorrelationFilter extends OncePerRequestFilter {
    private final Telemetry telemetry;

    public CorrelationFilter(Telemetry telemetry) {
        this.telemetry = telemetry;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        UUID correlation;
        String parent = request.getHeader("traceparent");
        try {
            String header = request.getHeader("X-Correlation-ID");
            correlation = header == null ? UUID.randomUUID() : UUID.fromString(header);
            if (parent != null && !parent.matches("00-(?!0{32}-)[0-9a-f]{32}-(?!0{16}-)[0-9a-f]{16}-[0-9a-f]{2}"))
                throw new IllegalArgumentException("Invalid trace context");
        } catch (IllegalArgumentException error) {
            response.setStatus(400);
            response.setContentType("application/problem+json");
            response.getWriter().write("{\"status\":400,\"detail\":\"Invalid correlation or trace context\"}");
            return;
        }
        request.setAttribute("correlationId", correlation);
        response.setHeader("X-Correlation-ID", correlation.toString());
        // Do not generate exporter traffic for health probes and metrics scrapes.
        if (request.getRequestURI().startsWith("/actuator/")) {
            chain.doFilter(request, response);
            return;
        }
        try (var trace = telemetry.start("http.server", SpanKind.SERVER, parent)) {
            MDC.put("correlationId", correlation.toString());
            trace.attribute("http.request.method", request.getMethod());
            String outgoing = Telemetry.currentTraceparentOr(parent);
            response.setHeader("traceparent", outgoing);
            var wrapped = new HttpServletRequestWrapper(request) {
                @Override
                public String getHeader(String name) {
                    if (name.equalsIgnoreCase("traceparent")) return outgoing;
                    if (name.equalsIgnoreCase("X-Correlation-ID")) return correlation.toString();
                    return super.getHeader(name);
                }

                @Override
                public Enumeration<String> getHeaders(String name) {
                    if (name.equalsIgnoreCase("traceparent") || name.equalsIgnoreCase("X-Correlation-ID"))
                        return Collections.enumeration(List.of(getHeader(name)));
                    return super.getHeaders(name);
                }

                @Override
                public Enumeration<String> getHeaderNames() {
                    var names = new LinkedHashSet<>(Collections.list(super.getHeaderNames()));
                    names.add("traceparent");
                    names.add("X-Correlation-ID");
                    return Collections.enumeration(names);
                }
            };
            try {
                chain.doFilter(wrapped, response);
                if (response.getStatus() >= 500) trace.failed();
            } catch (IOException | ServletException | RuntimeException error) {
                trace.failed();
                throw error;
            }
        }
    }
}
