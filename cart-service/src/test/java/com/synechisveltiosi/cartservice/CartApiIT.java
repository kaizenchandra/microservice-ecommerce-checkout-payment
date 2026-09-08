package com.synechisveltiosi.cartservice;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class CartApiIT {
    private static final String CUSTOMER = "11111111-1111-1111-1111-111111111111";
    private static final String OTHER = "22222222-2222-2222-2222-222222222222";
    private static final String PASSWORD = "integration-test-only";
    private static final PostgreSQLContainer DATABASE = new PostgreSQLContainer("postgres:17.6-alpine");
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static ConfigurableApplicationContext context;
    private static String base;
    private static com.sun.net.httpserver.HttpServer catalog;
    private static final java.util.concurrent.atomic.AtomicInteger catalogStatus = new java.util.concurrent.atomic.AtomicInteger(200);
    private static final java.util.concurrent.atomic.AtomicBoolean catalogActive = new java.util.concurrent.atomic.AtomicBoolean(true);
    private static final java.util.concurrent.atomic.AtomicLong catalogDelay = new java.util.concurrent.atomic.AtomicLong();
    private static final UUID PRODUCT = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1");

    @BeforeAll
    static void start() throws Exception {
        DATABASE.start();
        catalog = com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        catalog.createContext("/api/products/", exchange -> {
            int status = catalogStatus.get();
            try { Thread.sleep(catalogDelay.get()); }
            catch (InterruptedException error) { Thread.currentThread().interrupt(); }
            String expectedAuth = "Basic " + Base64.getEncoder().encodeToString((CUSTOMER + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8));
            if (!expectedAuth.equals(exchange.getRequestHeaders().getFirst("Authorization"))) { status = 401; }
            String productId = exchange.getRequestURI().getPath().substring("/api/products/".length());
            String response = "{\"id\":\"" + productId + "\",\"active\":" + catalogActive.get() + "}";
            byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            try (var output = exchange.getResponseBody()) { output.write(bytes); }
        });
        catalog.start();
        context = SpringApplication.run(CartServiceApplication.class,
                "--server.port=0", "--spring.profiles.active=demo",
                "--spring.datasource.url=" + DATABASE.getJdbcUrl(),
                "--spring.datasource.username=" + DATABASE.getUsername(),
                "--spring.datasource.password=" + DATABASE.getPassword(),
                "--demo.auth.customer-password=" + PASSWORD,
                "--demo.auth.second-customer-password=" + PASSWORD,
                "--demo.auth.admin-password=" + PASSWORD , "--catalog.base-url=http://127.0.0.1:" + catalog.getAddress().getPort());
        base = "http://localhost:" + context.getEnvironment().getProperty("local.server.port");
    }

    @AfterAll
    static void stop() {
        if (context != null) { context.close(); }
        if (catalog != null) { catalog.stop(0); }
        DATABASE.stop();
    }

    private static HttpResponse<String> request(String method, String path, String user, String body) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json");
        if (user != null) {
            builder.header("Authorization", "Basic " + Base64.getEncoder()
                    .encodeToString((user + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8)));
        }
        return HTTP.send(builder.method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private static JsonNode body(HttpResponse<String> response) { return JSON.readTree(response.body()); }
    
    private String newCart() throws Exception {
        var created = request("POST", "/api/carts", CUSTOMER, null);
        assertEquals(201, created.statusCode(), created.body());
        assertEquals(CUSTOMER, body(created).get("customerId").asString());
        return "/api/carts/" + body(created).get("id").asString();
    }

    @Test
    void enforcesOwnershipValidationAndVersionedMutations() throws Exception {
        assertEquals(401, request("POST", "/api/carts", null, null).statusCode());
        assertEquals(403, request("POST", "/api/carts", "admin", null).statusCode());
        String path = newCart();
        assertEquals(404, request("GET", path, OTHER, null).statusCode());
        String add = "{\"productId\":\"" + PRODUCT + "\",\"quantity\":2,\"expectedVersion\":0}";
        assertEquals(404, request("POST", path + "/items", OTHER, add).statusCode());
        var added = request("POST", path + "/items", CUSTOMER, add);
        assertEquals(200, added.statusCode(), added.body());
        assertEquals(1, body(added).get("version").asLong());
        assertEquals(2, body(added).get("items").get(0).get("quantity").asInt());
        assertEquals(409, request("POST", path + "/items", CUSTOMER, add).statusCode());
        var set = request("PUT", path + "/items/" + PRODUCT, CUSTOMER, "{\"quantity\":5,\"expectedVersion\":1}");
        assertEquals(200, set.statusCode(), set.body());
        assertEquals(2, body(set).get("version").asLong());
        assertEquals(400, request("PUT", path + "/items/" + PRODUCT, CUSTOMER, "{\"quantity\":0,\"expectedVersion\":2}").statusCode());
        assertEquals(400, request("PUT", path + "/items/" + PRODUCT, CUSTOMER, "{\"quantity\":1.5,\"expectedVersion\":2}").statusCode());
        var removed = request("DELETE", path + "/items/" + PRODUCT + "?expectedVersion=2", CUSTOMER, null);
        assertEquals(200, removed.statusCode(), removed.body());
        assertEquals(3, body(removed).get("version").asLong());
        assertTrue(body(removed).get("items").isEmpty());
    }

    @Test
    void catalogFailureDoesNotMutateCart() throws Exception {
        String path = newCart();
        String add = "{\"productId\":\"" + PRODUCT + "\",\"quantity\":2,\"expectedVersion\":0}";
        try {
            catalogStatus.set(503);
            assertEquals(503, request("POST", path + "/items", CUSTOMER, add).statusCode());
            catalogStatus.set(404);
            assertEquals(404, request("POST", path + "/items", CUSTOMER, add).statusCode());
            catalogStatus.set(200);
            catalogActive.set(false);
            assertEquals(409, request("POST", path + "/items", CUSTOMER, add).statusCode());
            catalogActive.set(true);
            catalogDelay.set(4000);
            assertEquals(503, request("POST", path + "/items", CUSTOMER, add).statusCode());
            var cart = body(request("GET", path, CUSTOMER, null));
            assertEquals(0, cart.get("version").asLong());
            assertTrue(cart.get("items").isEmpty());
        } finally { catalogStatus.set(200); catalogActive.set(true); catalogDelay.set(0); }
    }

    @Test
    void twoConcurrentTransactionsCannotLoseAnUpdate() throws Exception {
        String path = newCart();
        UUID cartId = UUID.fromString(path.substring("/api/carts/".length()));
        var repository = context.getBean(com.synechisveltiosi.cartservice.infrastructure.CartRepository.class);
        var manager = context.getBean(org.springframework.transaction.PlatformTransactionManager.class);
        var barrier = new java.util.concurrent.CyclicBarrier(2);
        java.util.concurrent.Callable<Boolean> change = () -> {
            try {
                new org.springframework.transaction.support.TransactionTemplate(manager).executeWithoutResult(status -> {
                    var cart = repository.findById(cartId).orElseThrow();
                    cart.items();
                    try { barrier.await(10, java.util.concurrent.TimeUnit.SECONDS); }
                    catch (Exception error) { throw new IllegalStateException(error); }
                    cart.add(UUID.randomUUID(), 1);
                    repository.flush();
                });
                return true;
            } catch (org.springframework.dao.OptimisticLockingFailureException conflict) { return false; }
        };
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var first = executor.submit(change);
            var second = executor.submit(change);
            assertNotEquals(first.get(15, java.util.concurrent.TimeUnit.SECONDS), second.get(15, java.util.concurrent.TimeUnit.SECONDS));
        }
        var cart = body(request("GET", path, CUSTOMER, null));
        assertEquals(1, cart.get("version").asLong());
        assertEquals(1, cart.get("items").size());
    }

}
