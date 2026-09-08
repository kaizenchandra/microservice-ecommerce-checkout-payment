package com.synechisveltiosi.orderservice.infrastructure;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CorrelationFilter extends OncePerRequestFilter {
    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        UUID correlation;
        String trace = request.getHeader("traceparent");
        try {
            String header = request.getHeader("X-Correlation-ID");
            correlation = header == null ? UUID.randomUUID() : UUID.fromString(header);
            if (trace != null && !trace.matches("00-(?!0{32}-)[0-9a-f]{32}-(?!0{16}-)[0-9a-f]{16}-[0-9a-f]{2}")) {
                throw new IllegalArgumentException("Invalid trace context");
            }
        } catch (IllegalArgumentException error) {
            response.setStatus(400);
            response.setContentType("application/problem+json");
            response.getWriter().write("{\"type\":\"about:blank\",\"title\":\"Bad Request\",\"status\":400,\"detail\":\"Invalid correlation or trace context\"}");
            return;
        }
        response.setHeader("X-Correlation-ID", correlation.toString());
        request.setAttribute("correlationId", correlation);
        try (var correlationScope = MDC.putCloseable("correlationId", correlation.toString())) {
            if (trace == null) { chain.doFilter(request, response); }
            else {
                try (var traceScope = MDC.putCloseable("traceId", trace.substring(3, 35))) { chain.doFilter(request, response); }
            }
        }
    }
}
