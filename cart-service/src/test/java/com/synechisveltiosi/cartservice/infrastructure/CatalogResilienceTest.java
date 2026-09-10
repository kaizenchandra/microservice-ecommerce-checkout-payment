package com.synechisveltiosi.cartservice.infrastructure;

import com.sun.net.httpserver.HttpServer;
import com.synechisveltiosi.cartservice.application.ApiException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class CatalogResilienceTest {
    @Test void outageOpensCircuitAndSuccessfulProbeRestoresCatalogAccess() throws Exception {
        var calls = new AtomicInteger(); var status = new AtomicInteger(503); var id = UUID.randomUUID();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/products/", exchange -> {
            calls.incrementAndGet();
            byte[] body = ("{\"id\":\"" + id + "\",\"active\":true}").getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(status.get(), body.length); exchange.getResponseBody().write(body); exchange.close();
        }); server.start();
        try {
            var client = new ProductCatalogClient(RestClient.builder(), "http://127.0.0.1:" + server.getAddress().getPort());
            for (int i = 0; i < 5; i++) assertThrows(ApiException.class, () -> client.requireActive(id, "Basic test"));
            assertEquals(CircuitBreaker.State.OPEN, client.breaker.getState());
            assertThrows(ApiException.class, () -> client.requireActive(id, "Basic test")); assertEquals(5, calls.get());
            status.set(200); client.breaker.transitionToHalfOpenState();
            client.requireActive(id, "Basic test"); assertEquals(CircuitBreaker.State.CLOSED, client.breaker.getState());
            assertEquals(6, calls.get());
        } finally { server.stop(0); }
    }
}
