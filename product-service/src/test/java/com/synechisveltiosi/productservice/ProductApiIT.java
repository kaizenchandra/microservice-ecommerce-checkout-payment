package com.synechisveltiosi.productservice;

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

class ProductApiIT {
    private static final String CUSTOMER = "11111111-1111-1111-1111-111111111111";
    private static final String OTHER = "22222222-2222-2222-2222-222222222222";
    private static final String PASSWORD = "integration-test-only";
    private static final PostgreSQLContainer DATABASE = new PostgreSQLContainer("postgres:17.6-alpine");
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static ConfigurableApplicationContext context;
    private static String base;


    @BeforeAll
    static void start() throws Exception {
        DATABASE.start();

        context = SpringApplication.run(ProductServiceApplication.class,
                "--server.port=0", "--spring.profiles.active=demo",
                "--spring.datasource.url=" + DATABASE.getJdbcUrl(),
                "--spring.datasource.username=" + DATABASE.getUsername(),
                "--spring.datasource.password=" + DATABASE.getPassword(),
                "--demo.auth.customer-password=" + PASSWORD,
                "--demo.auth.second-customer-password=" + PASSWORD,
                "--demo.auth.admin-password=" + PASSWORD);
        base = "http://localhost:" + context.getEnvironment().getProperty("local.server.port");
    }

    @AfterAll
    static void stop() {
        if (context != null) {
            context.close();
        }

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

    private static JsonNode body(HttpResponse<String> response) {
        return JSON.readTree(response.body());
    }

    @Test
    void seededCatalogIsPaginatedAndProtected() throws Exception {
        assertEquals(401, request("GET", "/api/products", null, null).statusCode());
        var page = request("GET", "/api/products?page=0&size=2", CUSTOMER, null);
        assertEquals(200, page.statusCode());
        assertEquals(2, body(page).get("items").size());
        assertTrue(body(page).get("totalElements").asLong() >= 4);
        assertEquals(400, request("GET", "/api/products?size=101", CUSTOMER, null).statusCode());
        assertEquals(404, request("GET", "/api/products/" + UUID.randomUUID(), CUSTOMER, null).statusCode());
        assertEquals(200, request("GET", "/actuator/health/readiness", null, null).statusCode());
    }

    @Test
    void adminCreatesUpdatesAndRejectsDuplicatesAndStaleWrites() throws Exception {
        String sku = "TEST-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        String create = """
                {"sku":"%s","name":"Test Product","description":"Example","price":{"amount":19.90,"currency":"USD"}}
                """.formatted(sku);
        assertEquals(403, request("POST", "/api/products", CUSTOMER, create).statusCode());
        var created = request("POST", "/api/products", "admin", create);
        assertEquals(201, created.statusCode(), created.body());
        assertEquals("19.90", body(created).get("price").get("amount").decimalValue().setScale(2).toPlainString());
        String path = created.headers().firstValue("Location").orElseThrow();
        assertEquals(409, request("POST", "/api/products", "admin", create).statusCode());
        String update = """
                {"name":"New name","description":"Changed","price":{"amount":29.90,"currency":"USD"},"active":false,"expectedVersion":0}
                """;
        var changed = request("PUT", path, "admin", update);
        assertEquals(200, changed.statusCode(), changed.body());
        assertEquals(1, body(changed).get("version").asLong());
        assertFalse(body(changed).get("active").asBoolean());
        assertEquals(409, request("PUT", path, "admin", update).statusCode());
        assertEquals("New name", body(request("GET", path, CUSTOMER, null)).get("name").asString());
    }

    @Test
    void validationReturnsProblemDetailsWithoutPersistenceInternals() throws Exception {
        var bad = request("POST", "/api/products", "admin", """
                {"sku":"bad sku","name":"","description":"","price":{"amount":1.001,"currency":"EUR"}}
                """);
        assertEquals(400, bad.statusCode());
        assertTrue(bad.headers().firstValue("Content-Type").orElseThrow().contains("application/problem+json"));
        assertTrue(body(bad).get("errors").size() >= 3);
        assertFalse(bad.body().contains("stackTrace"));
        assertEquals(400, request("POST", "/api/products", "admin", "{").statusCode());
    }

}
