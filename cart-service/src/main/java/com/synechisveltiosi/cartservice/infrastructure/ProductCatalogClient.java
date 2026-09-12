package com.synechisveltiosi.cartservice.infrastructure;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.synechisveltiosi.cartservice.application.ApiException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Semaphore;

@Component
public class ProductCatalogClient {
    final CircuitBreaker breaker = CircuitBreaker.of("catalog", CircuitBreakerConfig.custom()
            .slidingWindowSize(10).minimumNumberOfCalls(5).failureRateThreshold(50)
            .waitDurationInOpenState(Duration.ofSeconds(10)).permittedNumberOfCallsInHalfOpenState(1)
            .ignoreException(error -> error instanceof RestClientResponseException response &&
                    response.getStatusCode().is4xxClientError() && response.getStatusCode().value() != 429)
            .build());
    private final RestClient client;
    private final Semaphore permits = new Semaphore(24);

    public ProductCatalogClient(RestClient.Builder builder, @Value("${catalog.base-url}") String baseUrl) {
        var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        var factory = new JdkClientHttpRequestFactory(http);
        factory.setReadTimeout(Duration.ofSeconds(3));
        this.client = builder.baseUrl(baseUrl).requestFactory(factory).build();
    }

    public void requireActive(UUID id, String authorization) {
        if (!permits.tryAcquire()) throw unavailable();
        CatalogProduct product;
        try {
            product = breaker.executeSupplier(() -> client.get().uri("/api/products/{id}", id).header("Authorization", authorization)
                    .headers(headers -> {
                        String trace = Telemetry.currentTraceparentOr(null);
                        if (trace != null) headers.set("traceparent", trace);
                        String correlation = org.slf4j.MDC.get("correlationId");
                        if (correlation != null) headers.set("X-Correlation-ID", correlation);
                    })
                    .retrieve().body(CatalogProduct.class));
        } catch (RestClientResponseException error) {
            if (error.getStatusCode().value() == 404) {
                throw new ApiException(HttpStatus.NOT_FOUND, "PRODUCT_NOT_FOUND", "Product not found");
            }
            throw unavailable();
        } catch (RestClientException | CallNotPermittedException error) {
            throw unavailable();
        } finally {
            permits.release();
        }
        if (product == null || !id.equals(product.id()) || product.active() == null) {
            throw unavailable();
        }
        if (!product.active()) {
            throw new ApiException(HttpStatus.CONFLICT, "PRODUCT_INACTIVE", "Product is not available for purchase");
        }
    }

    private ApiException unavailable() {
        return new ApiException(HttpStatus.SERVICE_UNAVAILABLE, "CATALOG_UNAVAILABLE", "Catalog is temporarily unavailable; cart was not changed");
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record CatalogProduct(UUID id, Boolean active) {
    }
}
