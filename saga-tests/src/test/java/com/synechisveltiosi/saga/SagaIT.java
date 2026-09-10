package com.synechisveltiosi.saga;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Actual packaged applications, independent databases, one temporary Kafka broker; no shared-stack writes.
 */
class SagaIT {
    static final PostgreSQLContainer DB = new PostgreSQLContainer("postgres:17.6-alpine");
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1");
    static final JsonMapper JSON = JsonMapper.builder().build();
    static final String CUSTOMER = "11111111-1111-1111-1111-111111111111";
    static final String PASSWORD = "isolated-saga-only";
    static final List<Process> PROCESSES = new ArrayList<>();
    static final Map<String, Integer> PORTS = new LinkedHashMap<>();
    static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    @TempDir
    static Path logs;
    static String gateway;

    @BeforeAll
    static void start() throws Exception {
        DB.start();
        KAFKA.start();
        var kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", KAFKA.getBootstrapServers());
        try (var admin = org.apache.kafka.clients.admin.AdminClient.create(kafkaProperties)) {
            admin.createTopics(List.of("order.events", "inventory.events", "payment.events", "shipping.events", "notification.events").stream()
                    .map(topic -> new org.apache.kafka.clients.admin.NewTopic(topic, 3, (short) 1)).toList()).all().get(20, TimeUnit.SECONDS);
        }
        for (String service : List.of("order", "inventory", "payment", "shipping", "notification", "query")) {
            try (var connection = DriverManager.getConnection(DB.getJdbcUrl(), DB.getUsername(), DB.getPassword()); var statement = connection.createStatement()) {
                statement.execute("CREATE DATABASE " + service + "_saga");
            }
            startService(service.equals("query") ? "order-query-service" : service + "-service", service);
        }
        startService("api-gateway", null);
        gateway = "http://localhost:" + PORTS.get("api-gateway");
    }

    static String database(String service) {
        return "jdbc:postgresql://" + DB.getHost() + ":" + DB.getMappedPort(5432) + "/" + service + "_saga";
    }

    static void startService(String module, String database) throws Exception {
        int port;
        try (var socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        PORTS.put(module, port);
        var root = Path.of("..").toAbsolutePath().normalize();
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx192m", "-XX:ActiveProcessorCount=2", "-jar",
                root.resolve(module + "/target/" + module + "-1.0.0-SNAPSHOT.jar").toString(), "--server.port=" + port,
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(), "--demo.auth.customer-password=" + PASSWORD,
                "--demo.auth.second-customer-password=" + PASSWORD, "--demo.auth.admin-password=" + PASSWORD, "--demo.auth.checkout-password=" + PASSWORD));
        if (database != null)
            command.addAll(List.of("--spring.datasource.url=" + database(database), "--spring.datasource.username=" + DB.getUsername(), "--spring.datasource.password=" + DB.getPassword()));
        if (module.equals("order-query-service")) for (String owner : List.of("payment", "inventory", "shipping"))
            command.add("--details." + owner + "-url=http://localhost:" + PORTS.get(owner + "-service"));
        if (module.equals("api-gateway")) for (var entry : PORTS.entrySet())
            if (!entry.getKey().equals("api-gateway"))
                command.add("--" + entry.getKey().replace("-", "_").toUpperCase(Locale.ROOT) + "_URL=http://localhost:" + entry.getValue());
        Path log = logs.resolve(module + ".log");
        Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        PROCESSES.add(process);
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) fail(module + " exited; log: " + log + "\n" + tail(log));
            try {
                var response = HTTP.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/actuator/health/readiness")).timeout(Duration.ofSeconds(2)).build(), HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) return;
            } catch (Exception ignored) {
            }
            Thread.sleep(200);
        }
        fail(module + " did not become ready: " + tail(log));
    }

    static String tail(Path log) throws Exception {
        String text = Files.readString(log);
        return text.substring(Math.max(0, text.length() - 12000));
    }

    @AfterAll
    static void stop() throws Exception {
        for (var process : PROCESSES) process.destroy();
        for (var process : PROCESSES) if (!process.waitFor(10, TimeUnit.SECONDS)) process.destroyForcibly();
        KAFKA.stop();
        DB.stop();
    }

    static JsonNode request(String method, String path, String user, Object body, int status) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create(gateway + path)).timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json").header("Idempotency-Key", UUID.randomUUID().toString());
        if (user != null)
            builder.header("Authorization", "Basic " + Base64.getEncoder().encodeToString((user + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8)));
        var response = HTTP.send(builder.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body))).build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(status, response.statusCode(), path + ": " + response.body());
        return JSON.readTree(response.body());
    }

    static Order create(String token, String country, String postal, int stock) throws Exception {
        var product = UUID.randomUUID();
        var order = UUID.randomUUID();
        request("POST", "/api/inventory/stock", "admin", Map.of("productId", product, "onHand", stock), 201);
        var payload = Map.of("orderId", order, "customerId", CUSTOMER, "cartId", UUID.randomUUID(), "cartVersion", 1,
                "items", List.of(Map.of("productId", product, "sku", "SAGA-1", "name", "Synthetic saga item", "quantity", 1, "unitPrice", Map.of("amount", "25.00", "currency", "USD"))),
                "paymentToken", token, "shippingAddress", Map.of("recipient", "Demo Buyer", "line1", "Test Street", "city", "Test City", "postalCode", postal, "country", country));
        request("POST", "/api/orders", "checkout", payload, 202);
        return new Order(order, product);
    }

    static JsonNode awaitOrder(Order order, String status) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        JsonNode last = null;
        while (System.nanoTime() < deadline) {
            last = request("GET", "/api/orders/" + order.id(), CUSTOMER, null, 200);
            if (last.get("status").asString().equals(status)) return last;
            Thread.sleep(100);
        }
        fail("Order did not reach " + status + ": " + last + "; logs: " + logs);
        return null;
    }

    static long scalar(String service, String sql, UUID id) throws Exception {
        try (var connection = DriverManager.getConnection(database(service), DB.getUsername(), DB.getPassword()); var statement = connection.prepareStatement(sql)) {
            statement.setObject(1, id);
            try (var result = statement.executeQuery()) {
                result.next();
                return result.getLong(1);
            }
        }
    }

    static void awaitNotification(Order order, String status) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (scalar("notification", "SELECT count(*) FROM notification WHERE order_id = ?", order.id()) == 0 && System.nanoTime() < deadline)
            Thread.sleep(100);
        assertEquals(status, request("GET", "/api/notification/" + order.id(), "admin", null, 200).get("orderStatus").asString());
        assertEquals(1, scalar("notification", "SELECT count(*) FROM notification WHERE order_id = ?", order.id()));
    }

    static JsonNode awaitView(Order order, String status) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            var page = request("GET", "/api/order-views?size=100", CUSTOMER, null, 200);
            for (var view : page.get("items")) {
                if (view.get("orderId").asString().equals(order.id().toString()) &&
                        view.get("status").asString().equals(status) && view.get("notified").asBoolean()) return view;
            }
            Thread.sleep(100);
        }
        fail("Projection did not catch up; logs: " + logs);
        return null;
    }

    static void replayOutbox(String service, UUID id, String topic) throws Exception {
        var properties = new Properties();
        properties.put("bootstrap.servers", KAFKA.getBootstrapServers());
        properties.put("acks", "all");
        try (var producer = new KafkaProducer<String, String>(properties, new StringSerializer(), new StringSerializer());
             var connection = DriverManager.getConnection(database(service), DB.getUsername(), DB.getPassword());
             var statement = connection.prepareStatement("SELECT payload::text FROM outbox_event WHERE aggregate_id = ? ORDER BY aggregate_version")) {
            statement.setObject(1, id);
            try (var result = statement.executeQuery()) {
                while (result.next())
                    producer.send(new ProducerRecord<>(topic, id.toString(), result.getString(1))).get(10, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void successCompletesShipsAndNotifiesOnceDespiteRedelivery() throws Exception {
        var order = create("tok_success", "US", "12345", 2);
        awaitOrder(order, "COMPLETED");
        awaitNotification(order, "COMPLETED");
        assertEquals("CREATED", request("GET", "/api/shipping/" + order.id(), "admin", null, 200).get("status").asString());
        assertEquals(1, scalar("payment", "SELECT charge_count FROM provider_charge WHERE payment_id = ?", order.id()));
        replayOutbox("order", order.id(), "order.events");
        replayOutbox("inventory", order.id(), "inventory.events");
        replayOutbox("payment", order.id(), "payment.events");
        Thread.sleep(1500);
        assertEquals(1, scalar("shipping", "SELECT count(*) FROM shipping WHERE order_id = ?", order.id()));
        assertEquals(1, scalar("notification", "SELECT count(*) FROM notification WHERE order_id = ?", order.id()));
        assertEquals(1, scalar("payment", "SELECT charge_count FROM provider_charge WHERE payment_id = ?", order.id()));
        request("GET", "/api/shipping/" + order.id(), CUSTOMER, null, 403);
    }

    @Test
    void inventoryRejectionCancelsWithoutCharging() throws Exception {
        var order = create("tok_success", "US", "12345", 0);
        awaitOrder(order, "CANCELLED");
        awaitNotification(order, "CANCELLED");
        assertEquals(0, scalar("payment", "SELECT count(*) FROM payment WHERE payment_id = ?", order.id()));
    }

    @Test
    void declinedPaymentReleasesStockBeforeCancellation() throws Exception {
        var order = create("tok_declined", "US", "12345", 1);
        awaitOrder(order, "CANCELLED");
        awaitNotification(order, "CANCELLED");
        assertEquals(0, scalar("inventory", "SELECT reserved FROM stock WHERE product_id = ?", order.product()));
        assertEquals(0, scalar("payment", "SELECT charge_count FROM provider_charge WHERE payment_id = ?", order.id()));
        assertEquals(0, scalar("shipping", "SELECT count(*) FROM shipping WHERE order_id = ?", order.id()));
    }

    @Test
    void failedShipmentWaitsForRetriedRefundAndRelease() throws Exception {
        var order = create("tok_success", "ZZ", "REFUND-RETRY", 1);
        long deadline = System.nanoTime() + Duration.ofSeconds(40).toNanos();
        boolean sawCompensating = false;
        while (System.nanoTime() < deadline) {
            String status = request("GET", "/api/orders/" + order.id(), CUSTOMER, null, 200).get("status").asString();
            if (status.equals("COMPENSATING")) {
                sawCompensating = true;
                break;
            }
            Thread.sleep(50);
        }
        assertTrue(sawCompensating);
        awaitOrder(order, "CANCELLED");
        awaitNotification(order, "CANCELLED");
        assertEquals(1, scalar("payment", "SELECT refund_count FROM provider_refund WHERE payment_id = ?", order.id()));
        assertEquals(3, scalar("payment", "SELECT requests FROM provider_refund WHERE payment_id = ?", order.id()));
        assertEquals(0, scalar("inventory", "SELECT reserved FROM stock WHERE product_id = ?", order.product()));
        assertEquals("REFUNDED", request("GET", "/api/payments/" + order.id() + "/refund", "admin", null, 200).get("status").asString());
    }

    @Test
    void responseLossDuringChargeAndRefundDoesNotDuplicateEffects() throws Exception {
        var order = create("tok_timeout", "ZZ", "REFUND-TIMEOUT", 1);
        awaitOrder(order, "CANCELLED");
        awaitNotification(order, "CANCELLED");
        assertEquals(1, scalar("payment", "SELECT charge_count FROM provider_charge WHERE payment_id = ?", order.id()));
        assertEquals(1, scalar("payment", "SELECT requests FROM provider_charge WHERE payment_id = ?", order.id()));
        assertEquals(1, scalar("payment", "SELECT refund_count FROM provider_refund WHERE payment_id = ?", order.id()));
        assertEquals(1, scalar("payment", "SELECT requests FROM provider_refund WHERE payment_id = ?", order.id()));
    }

    @Test
    void queryProjectsActualSagaEventsAndRebuildsThroughGateway() throws Exception {
        var success = create("tok_success", "US", "12345", 1);
        var failed = create("tok_success", "ZZ", "12345", 1);
        awaitOrder(success, "COMPLETED");
        awaitOrder(failed, "CANCELLED");
        var completed = awaitView(success, "COMPLETED");
        var cancelled = awaitView(failed, "CANCELLED");
        assertEquals("CREATED", completed.get("shippingStatus").asString());
        assertEquals("REFUNDED", cancelled.get("refundStatus").asString());
        assertEquals("RELEASED", cancelled.get("inventoryStatus").asString());
        assertFalse(completed.toString().contains("tok_success"));
        request("GET", "/api/order-views/" + success.id(), "22222222-2222-2222-2222-222222222222", null, 404);
        var details = request("GET", "/api/order-views/" + success.id() + "/details", CUSTOMER, null, 200);
        for (String owner : List.of("payment", "inventory", "shipping"))
            assertEquals("AVAILABLE", details.get(owner).get("availability").asString());
        assertEquals("CREATED", details.get("shipping").get("data").get("status").asString());
        assertFalse(details.toString().contains("tok_success"));
        request("GET", "/api/order-views/" + success.id() + "/details", null, null, 401);
        request("GET", "/api/order-views/" + success.id() + "/details", "22222222-2222-2222-2222-222222222222", null, 404);
        request("GET", "/api/order-views/" + success.id() + "/details", "admin", null, 200);
        for (String path : List.of("/api/shipping/", "/api/inventory/reservations/")) {
            request("GET", path + success.id() + "/details", CUSTOMER, null, 200);
            request("GET", path + success.id() + "/details", "22222222-2222-2222-2222-222222222222", null, 404);
        }
        long generation = request("POST", "/api/order-views/admin/rebuild", "admin", null, 202).get("generation").asLong();
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (request("GET", "/api/order-views/admin/projection", "admin", null, 200).get("activeGeneration").asLong() != generation) {
            assertTrue(System.nanoTime() < deadline, "Rebuild did not finish");
            Thread.sleep(100);
        }
        assertEquals(completed, request("GET", "/api/order-views/" + success.id(), CUSTOMER, null, 200));
        assertEquals(cancelled, request("GET", "/api/order-views/" + failed.id(), CUSTOMER, null, 200));
    }

    record Order(UUID id, UUID product) {
    }
}
