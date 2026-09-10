package com.synechisveltiosi.orderqueryservice.infrastructure;

import com.sun.net.httpserver.HttpServer;
import com.synechisveltiosi.orderqueryservice.api.OrderQueryController;
import com.synechisveltiosi.orderqueryservice.application.ApiException;
import com.synechisveltiosi.orderqueryservice.application.OrderQueries;
import com.synechisveltiosi.orderqueryservice.application.ProjectionTransactions;
import com.synechisveltiosi.orderqueryservice.domain.OrderView;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.web.client.RestClient;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.synechisveltiosi.orderqueryservice.infrastructure.OrderDetailsClient.Availability.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class OrderDetailsClientTest {
    HttpServer server;
    ExecutorService serverThreads;
    OrderDetailsClient client;
    UUID order = UUID.randomUUID(), customer = UUID.randomUUID();
    OrderView view;
    String base;
    AtomicInteger calls = new AtomicInteger();

    @BeforeEach
    void start() throws Exception {
        view = new OrderView(order, customer, "PENDING", 1, List.of(), null, null, "WEB", List.of(),
                null, null, null, null, null, null, null, false, Map.of(), null, null);
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        serverThreads = Executors.newVirtualThreadPerTaskExecutor();
        server.setExecutor(serverThreads);
        server.start();
        base = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void stop() {
        if (client != null) client.close();
        if (server != null) server.stop(0);
        if (serverThreads != null) serverThreads.shutdownNow();
    }

    void client(long timeout) {
        client = new OrderDetailsClient(RestClient.builder(), base, base, base, timeout, 3);
    }

    String body() {
        return "{\"orderId\":\"" + order + "\",\"customerId\":\"" + customer + "\",\"status\":\"PENDING\",\"paymentToken\":\"must-not-expose\"}";
    }

    OrderDetailsClient.Details get() {
        return client.get(view, "Basic test", order.toString(), null);
    }

    void endpoint(String path, int code, String body) {
        server.createContext(path, exchange -> {
            calls.incrementAndGet();
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(code, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
    }

    @Test
    void fansOutInParallelAndForwardsCallerContext() {
        var arrived = new CountDownLatch(3);
        var headers = new ConcurrentLinkedQueue<String>();
        server.createContext("/api/", exchange -> {
            calls.incrementAndGet();
            headers.add(exchange.getRequestHeaders().getFirst("Authorization"));
            headers.add(exchange.getRequestHeaders().getFirst("X-Correlation-ID"));
            arrived.countDown();
            try {
                if (!arrived.await(2, TimeUnit.SECONDS)) throw new AssertionError("Lookups were not parallel");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            byte[] bytes = body().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        client(3000);
        var result = get();
        assertEquals(AVAILABLE, result.payment().availability());
        assertEquals(AVAILABLE, result.inventory().availability());
        assertEquals(AVAILABLE, result.shipping().availability());
        assertSame(view, result.projection());
        assertEquals(3, Collections.frequency(new ArrayList<>(headers), "Basic test"));
        assertEquals(3, Collections.frequency(new ArrayList<>(headers), order.toString()));
    }

    @Test
    void distinguishesMissingFromFailuresAndPreservesHealthyParts() {
        endpoint("/api/payments/", 404, "{}");
        endpoint("/api/inventory/", 503, "secret diagnostic");
        endpoint("/api/shipping/", 200, body());
        client(2000);
        var result = get();
        assertEquals(NOT_FOUND, result.payment().availability());
        assertNull(result.payment().data());
        assertEquals(UNAVAILABLE, result.inventory().availability());
        assertNull(result.inventory().data());
        assertEquals(AVAILABLE, result.shipping().availability());
    }

    @Test
    void rejectsMalformedWrongOwnerAndRedirectResponses() {
        endpoint("/api/payments/", 200, "invalid JSON");
        endpoint("/api/inventory/", 200, body().replace(customer.toString(), UUID.randomUUID().toString()));
        endpoint("/api/shipping/", 302, "{}");
        client(2000);
        var result = get();
        assertEquals(UNAVAILABLE, result.payment().availability());
        assertEquals(UNAVAILABLE, result.inventory().availability());
        assertEquals(UNAVAILABLE, result.shipping().availability());
    }

    @Test
    void boundsDeadlineAndConcurrentLookups() throws Exception {
        var arrived = new CountDownLatch(3);
        var release = new CountDownLatch(1);
        server.createContext("/api/", exchange -> {
            calls.incrementAndGet();
            arrived.countDown();
            try {
                release.await(3, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                exchange.close();
            }
        });
        client(500);
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            var first = callers.submit(this::get);
            assertTrue(arrived.await(2, TimeUnit.SECONDS));
            var second = get();
            assertEquals(UNAVAILABLE, second.payment().availability());
            assertEquals(UNAVAILABLE, second.inventory().availability());
            assertEquals(UNAVAILABLE, second.shipping().availability());
            var result = first.get(1500, TimeUnit.MILLISECONDS);
            assertEquals(UNAVAILABLE, result.payment().availability());
            assertEquals(3, calls.get());
        } finally {
            release.countDown();
        }
    }

    @Test
    void checksOwnershipBeforeAnyFanOut() {
        var queries = mock(OrderQueries.class);
        var remote = mock(OrderDetailsClient.class);
        when(queries.get(order, customer, false)).thenThrow(new ApiException(HttpStatus.NOT_FOUND, "ORDER_VIEW_NOT_FOUND", "Order view not found"));
        var controller = new OrderQueryController(queries, mock(ProjectionTransactions.class), remote);
        var auth = new UsernamePasswordAuthenticationToken(customer.toString(), "", List.of());
        assertThrows(ApiException.class, () -> controller.details(order, auth, new MockHttpServletRequest()));
        verifyNoInteractions(remote);
    }
}
