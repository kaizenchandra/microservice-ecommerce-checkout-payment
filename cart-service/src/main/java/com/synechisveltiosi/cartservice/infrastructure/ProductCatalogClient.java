package com.synechisveltiosi.cartservice.infrastructure;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.synechisveltiosi.cartservice.application.ApiException;
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

@Component
public class ProductCatalogClient {
    private final RestClient client;

    public ProductCatalogClient(RestClient.Builder builder, @Value("${catalog.base-url}") String baseUrl) {
        var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        var factory = new JdkClientHttpRequestFactory(http);
        factory.setReadTimeout(Duration.ofSeconds(3));
        this.client = builder.baseUrl(baseUrl).requestFactory(factory).build();
    }

    public void requireActive(UUID id, String authorization) {
        CatalogProduct product;
        try {
            product = client.get().uri("/api/products/{id}", id).header("Authorization", authorization)
                    .retrieve().body(CatalogProduct.class);
        } catch (RestClientResponseException error) {
            if (error.getStatusCode().value() == 404) {
                throw new ApiException(HttpStatus.NOT_FOUND, "PRODUCT_NOT_FOUND", "Product not found");
            }
            throw unavailable();
        } catch (RestClientException error) {
            throw unavailable();
        }
        if (product == null || !id.equals(product.id()) || product.active() == null) { throw unavailable(); }
        if (!product.active()) {
            throw new ApiException(HttpStatus.CONFLICT, "PRODUCT_INACTIVE", "Product is not available for purchase");
        }
    }

    private ApiException unavailable() {
        return new ApiException(HttpStatus.SERVICE_UNAVAILABLE, "CATALOG_UNAVAILABLE", "Catalog is temporarily unavailable; cart was not changed");
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record CatalogProduct(UUID id, Boolean active) { }
}
