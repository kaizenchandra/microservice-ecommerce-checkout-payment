package com.synechisveltiosi.orderqueryservice.infrastructure;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.synechisveltiosi.orderqueryservice.domain.OrderView;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

@Component
public class OrderDetailsClient {
    final Map<Class<?>, CircuitBreaker> breakers = Map.of(
            Payment.class, breaker("payment"), Inventory.class, breaker("inventory"), Shipping.class, breaker("shipping"));
    private final RestClient payment, inventory, shipping;
    private final HttpClient http;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    // No waiting queue: excess requests receive explicit partial availability.
    private final Semaphore permits;
    private final long timeoutMillis;

    public OrderDetailsClient(RestClient.Builder builder,
                              @Value("${details.payment-url:http://localhost:8086}") String paymentUrl,
                              @Value("${details.inventory-url:http://localhost:8085}") String inventoryUrl,
                              @Value("${details.shipping-url:http://localhost:8087}") String shippingUrl,
                              @Value("${details.timeout-ms:1000}") long timeoutMillis,
                              @Value("${details.max-concurrent-lookups:24}") int maxConcurrent) {
        if (timeoutMillis < 1 || timeoutMillis > 10000 || maxConcurrent < 3 || maxConcurrent > 300)
            throw new IllegalArgumentException("Invalid details lookup limits");
        this.timeoutMillis = timeoutMillis;
        permits = new Semaphore(maxConcurrent);
        http = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(timeoutMillis))
                .followRedirects(HttpClient.Redirect.NEVER).build();
        var factory = new JdkClientHttpRequestFactory(http);
        factory.setReadTimeout(Duration.ofMillis(timeoutMillis));
        payment = builder.clone().baseUrl(paymentUrl).requestFactory(factory).build();
        inventory = builder.clone().baseUrl(inventoryUrl).requestFactory(factory).build();
        shipping = builder.clone().baseUrl(shippingUrl).requestFactory(factory).build();
    }

    private static CircuitBreaker breaker(String name) {
        return CircuitBreaker.of(name, CircuitBreakerConfig.custom()
                .slidingWindowSize(10).minimumNumberOfCalls(5).failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10)).permittedNumberOfCallsInHalfOpenState(1)
                .ignoreException(error -> error instanceof RestClientResponseException response &&
                        response.getStatusCode().is4xxClientError() && response.getStatusCode().value() != 429)
                .build());
    }

    private static boolean matches(OrderView view, UUID order, UUID customer, String status) {
        return view.orderId().equals(order) && view.customerId().equals(customer) && status != null && !status.isBlank();
    }

    private static <T> Part<T> unavailable() {
        return new Part<>(Availability.UNAVAILABLE, null);
    }

    private static <T> Part<T> await(Future<Part<T>> future, long deadline) {
        try {
            return future.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            return unavailable();
        } catch (ExecutionException | TimeoutException | CancellationException error) {
            return unavailable();
        }
    }

    public Details get(OrderView view, String authorization, String correlation, String traceparent) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        var p = submit(payment, "/api/payments/", view, Payment.class, authorization, correlation, traceparent);
        var i = submit(inventory, "/api/inventory/reservations/", view, Inventory.class, authorization, correlation, traceparent);
        var s = submit(shipping, "/api/shipping/", view, Shipping.class, authorization, correlation, traceparent);
        try {
            return new Details(view, await(p, deadline), await(i, deadline), await(s, deadline));
        } finally {
            p.cancel(true);
            i.cancel(true);
            s.cancel(true);
        }
    }

    private <T> Future<Part<T>> submit(RestClient client, String path, OrderView view, Class<T> type,
                                       String authorization, String correlation, String traceparent) {
        try {
            return executor.submit(() -> {
                if (!permits.tryAcquire()) return unavailable();
                try {
                    var request = client.get().uri(path + view.orderId() + (type == Payment.class ? "" : "/details")).header("Authorization", authorization)
                            .header("X-Correlation-ID", correlation);
                    if (traceparent != null) request.header("traceparent", traceparent);
                    T data = breakers.get(type).executeSupplier(() -> request.retrieve().body(type));
                    boolean valid = switch (data) {
                        case Payment p -> matches(view, p.orderId(), p.customerId(), p.status());
                        case Inventory i -> matches(view, i.orderId(), i.customerId(), i.status());
                        case Shipping s -> matches(view, s.orderId(), s.customerId(), s.status());
                        case null, default -> false;
                    };
                    return valid ? new Part<>(Availability.AVAILABLE, data) : unavailable();
                } catch (RestClientResponseException error) {
                    return new Part<T>(error.getStatusCode().value() == 404 ? Availability.NOT_FOUND : Availability.UNAVAILABLE, null);
                } catch (RestClientException | IllegalArgumentException | CallNotPermittedException error) {
                    return unavailable();
                } finally {
                    permits.release();
                }
            });
        } catch (RejectedExecutionException error) {
            return CompletableFuture.completedFuture(unavailable());
        }
    }

    @PreDestroy
    void close() {
        executor.shutdownNow();
        http.shutdownNow();
    }

    public enum Availability {AVAILABLE, NOT_FOUND, UNAVAILABLE}

    public record Part<T>(Availability availability, T data) {
    }

    public record Details(OrderView projection, Part<Payment> payment, Part<Inventory> inventory,
                          Part<Shipping> shipping) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Payment(UUID orderId, UUID customerId, String status, UUID paymentId, UUID providerReference) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Inventory(UUID orderId, UUID customerId, String status, Long version, String reason) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Shipping(UUID orderId, UUID customerId, String status, String trackingNumber) {
    }
}
