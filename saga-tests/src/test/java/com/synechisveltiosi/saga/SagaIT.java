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
    static final Map<String, Process> SERVICE_PROCESSES = new HashMap<>();
    static final Map<String, Integer> PORTS = new LinkedHashMap<>();
    static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    @TempDir
    static Path logs;
    static String gateway;
    static final String JWT_SECRET = "isolated-saga-signing-key-minimum-32-bytes";
    static OtlpCapture traces;
    static String traceHeader;

    @BeforeAll
    static void start() throws Exception {
        traces = new OtlpCapture();
        DB.start();
        KAFKA.start();
        var kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", KAFKA.getBootstrapServers());
        try (var admin = org.apache.kafka.clients.admin.AdminClient.create(kafkaProperties)) {
            admin.createTopics(List.of("order.events", "inventory.events", "payment.events", "shipping.events", "notification.events").stream()
                    .map(topic -> new org.apache.kafka.clients.admin.NewTopic(topic, 3, (short) 1)).toList()).all().get(20, TimeUnit.SECONDS);
        }
        for (String service : List.of("product", "cart", "order", "inventory", "payment", "shipping", "notification", "query")) {
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
        if (PORTS.containsKey(module)) port = PORTS.get(module);
        else try (var socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
        PORTS.put(module, port);
        var root = Path.of("..").toAbsolutePath().normalize();
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx192m", "-XX:ActiveProcessorCount=2", "-jar",
                root.resolve(module + "/target/" + module + "-1.0.0-SNAPSHOT.jar").toString(), "--server.port=" + port, "--security.jwt.secret=" + JWT_SECRET, "--demo.auth.basic-enabled=false",
                "--telemetry.endpoint=" + traces.endpoint(),
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(), "--demo.auth.customer-password=" + PASSWORD,
                "--demo.auth.second-customer-password=" + PASSWORD, "--demo.auth.admin-password=" + PASSWORD, "--demo.auth.checkout-password=" + PASSWORD));
        if (database != null)
            command.addAll(List.of("--spring.datasource.url=" + database(database), "--spring.datasource.username=" + DB.getUsername(), "--spring.datasource.password=" + DB.getPassword()));
        if (module.equals("inventory-service")) command.add("--spring.kafka.listener.concurrency=3");
        if (module.equals("cart-service")) command.add("--catalog.base-url=http://localhost:" + PORTS.get("product-service"));
        if (module.equals("order-query-service")) for (String owner : List.of("payment", "inventory", "shipping"))
            command.add("--details." + owner + "-url=http://localhost:" + PORTS.get(owner + "-service"));
        if (module.equals("api-gateway")) for (var entry : PORTS.entrySet())
            if (!entry.getKey().equals("api-gateway"))
                command.add("--" + entry.getKey().replace("-", "_").toUpperCase(Locale.ROOT) + "_URL=http://localhost:" + entry.getValue());
        Path log = logs.resolve(module + ".log");
        Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        PROCESSES.add(process); SERVICE_PROCESSES.put(module, process);
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
        DB.stop(); if (traces != null) traces.close();
    }

    static JsonNode request(String method, String path, String user, Object body, int status) throws Exception {
        return request(method, path, user, body, status, UUID.randomUUID());
    }
    static JsonNode request(String method, String path, String user, Object body, int status, UUID key) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create(gateway + path)).timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json").header("Idempotency-Key", key.toString());
        if (user != null)
            builder.header("Authorization", "Bearer " + token(user, JWT_SECRET, "checkout-demo", "ecommerce-api", 900));
        if (traceHeader != null) builder.header("traceparent", traceHeader);
        var response = HTTP.send(builder.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body))).build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(status, response.statusCode(), path + ": " + response.body());
        return JSON.readTree(response.body());
    }

    static Order create(String token, String country, String postal, int stock) throws Exception {
        var product = UUID.randomUUID();
        var order = UUID.randomUUID();
        request("POST", "/api/inventory/stock", "admin", Map.of("productId", product, "onHand", stock), 201);
        var payload = orderCommand(order, product, token, country, postal);
        request("POST", "/api/orders", "checkout", payload, 202);
        return new Order(order, product);
    }

    static Map<String, Object> orderCommand(UUID order, UUID product, String token, String country, String postal) {
        return Map.of("orderId", order, "customerId", CUSTOMER, "cartId", UUID.randomUUID(), "cartVersion", 1,
                "items", List.of(Map.of("productId", product, "sku", "SAGA-1", "name", "Synthetic saga item", "quantity", 1, "unitPrice", Map.of("amount", "25.00", "currency", "USD"))),
                "paymentToken", token, "shippingAddress", Map.of("recipient", "Demo Buyer", "line1", "Test Street", "city", "Test City", "postalCode", postal, "country", country));
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
    @Test
    void failedProjectionIsDeadLetteredAndRedrivenWithoutRepeatingBusinessEffects() throws Exception {
        var props = new Properties(); props.put("bootstrap.servers", KAFKA.getBootstrapServers());
        props.put("enable.auto.commit", "false"); props.put("auto.offset.reset", "earliest");
        String topic = "order.events.order-query-service.DLT";
        try (var consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(props,
                new org.apache.kafka.common.serialization.StringDeserializer(), new org.apache.kafka.common.serialization.StringDeserializer());
             var connection = DriverManager.getConnection(database("query"), DB.getUsername(), DB.getPassword());
             var statement = connection.createStatement()) {
            var partitions = List.of(new org.apache.kafka.common.TopicPartition(topic, 0), new org.apache.kafka.common.TopicPartition(topic, 1), new org.apache.kafka.common.TopicPartition(topic, 2));
            consumer.assign(partitions); consumer.seekToEnd(partitions);
            for (var partition : partitions) consumer.position(partition);
            // A temporary database fault is isolated to the query projection.
            statement.execute("ALTER TABLE order_projection ADD CONSTRAINT test_dlt_failure CHECK (false) NOT VALID");
            Order order;
            org.apache.kafka.clients.consumer.ConsumerRecord<String, String> dead = null;
            try {
                order = create("tok_success", "US", "12345", 1);
                awaitOrder(order, "COMPLETED"); awaitNotification(order, "COMPLETED");
                long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
                while (dead == null && System.nanoTime() < deadline) {
                    for (var record : consumer.poll(Duration.ofMillis(250)))
                        if (order.id().toString().equals(record.key()) && JSON.readTree(record.value()).get("eventType").asString().equals("OrderCreated")) dead = record;
                }
                assertNotNull(dead, "Failed projection must reach its own DLT");
            } finally { statement.execute("ALTER TABLE order_projection DROP CONSTRAINT test_dlt_failure"); }
            String original = dead.value();
            var sourcePosition = new org.apache.kafka.common.TopicPartition("order.events", dead.partition());
            consumer.assign(List.of(sourcePosition)); consumer.seekToEnd(List.of(sourcePosition)); consumer.position(sourcePosition);
            for (boolean execute : List.of(false, true)) {
                var command = new ArrayList<>(List.of("python3", "../infrastructure/scripts/redrive.py", KAFKA.getBootstrapServers(), topic,
                        Integer.toString(dead.partition()), Long.toString(dead.offset())));
                if (execute) command.add("--execute");
                Path log = logs.resolve("redrive-" + execute + ".log");
                var process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
                try { assertTrue(process.waitFor(60, TimeUnit.SECONDS)); assertEquals(0, process.exitValue(), Files.readString(log)); }
                finally { if (process.isAlive()) process.destroyForcibly(); }
                if (!execute) assertEquals(0, scalar("query", "SELECT count(*) FROM order_projection WHERE order_id = ?", order.id()));
                else {
                    org.apache.kafka.clients.consumer.ConsumerRecord<String, String> replayed = null;
                    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                    while (replayed == null && System.nanoTime() < deadline)
                        for (var record : consumer.poll(Duration.ofMillis(250)))
                            if (dead.key().equals(record.key()) && JSON.readTree(record.value()).get("eventId").equals(JSON.readTree(original).get("eventId"))) replayed = record;
                    assertNotNull(replayed); assertEquals(original, replayed.value()); assertEquals(dead.key(), replayed.key());
                }
            }
            // Replay other query DLT facts too; gaps are deliberately preserved until recovery.
            replayOutbox("order", order.id(), "order.events");
            replayOutbox("inventory", order.id(), "inventory.events");
            replayOutbox("payment", order.id(), "payment.events");
            replayOutbox("shipping", order.id(), "shipping.events");
            replayOutbox("notification", order.id(), "notification.events");
            assertEquals("COMPLETED", awaitView(order, "COMPLETED").get("status").asString());
            assertEquals(1, scalar("payment", "SELECT charge_count FROM provider_charge WHERE payment_id = ?", order.id()));
            assertEquals(1, scalar("notification", "SELECT count(*) FROM notification WHERE order_id = ?", order.id()));
            assertEquals(order.id().toString(), JSON.readTree(original).get("aggregateId").asString());
        }
    }
    static String token(String subject, String secret, String issuer, String audience, int lifetime) throws Exception {
        var encode = Base64.getUrlEncoder().withoutPadding(); long now = java.time.Instant.now().getEpochSecond();
        String role = subject.equals("admin") ? "ADMIN" : subject.equals("checkout") ? "CHECKOUT" : "CUSTOMER";
        String body = encode.encodeToString(JSON.writeValueAsString(Map.of("alg", "HS256", "typ", "JWT")).getBytes(StandardCharsets.UTF_8)) + "." +
                encode.encodeToString(JSON.writeValueAsString(Map.of("iss", issuer, "aud", List.of(audience), "sub", subject, "roles", List.of(role), "iat", now, "exp", now + lifetime)).getBytes(StandardCharsets.UTF_8));
        var mac = javax.crypto.Mac.getInstance("HmacSHA256"); mac.init(new javax.crypto.spec.SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        return body + "." + encode.encodeToString(mac.doFinal(body.getBytes(StandardCharsets.UTF_8)));
    }
    @Test void jwtIsValidatedAtGatewayAndOwnerServices() throws Exception {
        var issuer = new ProcessBuilder("python3", "../infrastructure/scripts/demo_token.py", "--identity", "customer");
        issuer.environment().put("JWT_SECRET", JWT_SECRET); issuer.environment().put("JWT_ISSUER", "checkout-demo"); issuer.environment().put("JWT_AUDIENCE", "ecommerce-api");
        var process = issuer.start(); String valid;
        try { assertTrue(process.waitFor(10, TimeUnit.SECONDS)); assertEquals(0, process.exitValue()); valid = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim(); }
        finally { if (process.isAlive()) process.destroyForcibly(); }
        for (String base : List.of(gateway, "http://localhost:" + PORTS.get("order-service"), "http://localhost:" + PORTS.get("order-query-service"))) {
            String path = base.equals("http://localhost:" + PORTS.get("order-service")) ? "/api/orders/" : "/api/order-views/";
            for (String authorization : List.of("Basic " + Base64.getEncoder().encodeToString((CUSTOMER + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8)),
                    "Bearer " + token(CUSTOMER, JWT_SECRET, "wrong-issuer", "ecommerce-api", 900),
                    "Bearer " + token(CUSTOMER, JWT_SECRET, "checkout-demo", "wrong-audience", 900),
                    "Bearer " + token(CUSTOMER, JWT_SECRET, "checkout-demo", "ecommerce-api", -120),
                    "Bearer " + token(CUSTOMER, "wrong-signing-secret-at-least-32-bytes", "checkout-demo", "ecommerce-api", 900))) {
                var response = HTTP.send(HttpRequest.newBuilder(URI.create(base + path + UUID.randomUUID())).header("Authorization", authorization).build(), HttpResponse.BodyHandlers.ofString());
                assertEquals(401, response.statusCode()); assertFalse(response.body().contains(JWT_SECRET));
            }
            assertEquals(404, HTTP.send(HttpRequest.newBuilder(URI.create(base + path + UUID.randomUUID())).header("Authorization", "Bearer " + valid).build(), HttpResponse.BodyHandlers.ofString()).statusCode());
        }
    }
    @Test void exportedTraceConnectsHttpAndKafkaAcrossSaga() throws Exception {
        String traceId = UUID.randomUUID().toString().replace("-", ""); traceHeader = "00-" + traceId + "-1234567890abcdef-01";
        Order order;
        try { order = create("tok_success", "US", "12345", 1); awaitOrder(order, "COMPLETED"); awaitNotification(order, "COMPLETED"); awaitView(order, "COMPLETED"); }
        finally { traceHeader = null; }
        Set<String> expected = Set.of("api-gateway", "order-service", "inventory-service", "payment-service", "shipping-service", "notification-service", "order-query-service");
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        List<OtlpCapture.ExportedSpan> found;
        while (true) {
            found = traces.spans.stream().filter(span -> span.traceId().equals(traceId)).toList();
            var ids = found.stream().map(OtlpCapture.ExportedSpan::spanId).collect(java.util.stream.Collectors.toSet());
            if (expected.stream().allMatch(service -> foundService(traces, traceId, service)) &&
                    found.stream().filter(span -> span.name().equals("kafka.consume")).allMatch(span -> ids.contains(span.parentId()))) break;
            if (System.nanoTime() >= deadline) fail("Missing exported saga spans: " + found);
            Thread.sleep(100);
        }
        assertTrue(found.stream().anyMatch(span -> span.service().equals("api-gateway") && span.name().equals("http.server")));
        for (String service : expected) if (!service.equals("api-gateway"))
            assertTrue(found.stream().anyMatch(span -> span.service().equals(service) && span.name().equals("kafka.consume")), service);
        var ids = found.stream().map(OtlpCapture.ExportedSpan::spanId).collect(java.util.stream.Collectors.toSet());
        assertTrue(found.stream().filter(span -> span.name().equals("kafka.consume")).allMatch(span -> ids.contains(span.parentId())), "Every consumed span must reference an exported producer span");
        var metrics = HTTP.send(HttpRequest.newBuilder(URI.create("http://localhost:" + PORTS.get("payment-service") + "/actuator/prometheus")).build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, metrics.statusCode()); assertTrue(metrics.body().contains("payments_accepted_total"));
        assertTrue(metrics.body().contains("messaging_processed_total")); assertFalse(metrics.body().contains(order.id().toString()));
        assertTrue(Files.readAllLines(logs.resolve("inventory-service.log")).stream()
                .anyMatch(line -> line.contains(traceId) && line.contains("eventId") && line.contains("spanId")), "Consumer logs must carry trace/span/event context together");
    }
    static boolean foundService(OtlpCapture traces, String traceId, String service) {
        return traces.spans.stream().anyMatch(span -> span.traceId().equals(traceId) && span.service().equals(service) &&
                span.name().equals(service.equals("api-gateway") ? "http.server" : "kafka.consume"));
    }
    @Test void jwtCatalogCartFlowSurvivesRealCatalogOutageWithoutMutation() throws Exception {
        var product = request("POST", "/api/products", "admin", Map.of("sku", "P12-" + UUID.randomUUID().toString().replace("-", "").toUpperCase(Locale.ROOT),
                "name", "Phase 12 fixture", "description", "Isolated catalog fixture", "price", Map.of("amount", "12.50", "currency", "USD")), 201);
        String productId = product.get("id").asString();
        var cart = request("POST", "/api/carts", CUSTOMER, null, 201);
        String path = "/api/carts/" + cart.get("id").asString();
        request("GET", path, null, null, 401);
        request("GET", path, "22222222-2222-2222-2222-222222222222", null, 404);
        var add = Map.of("productId", productId, "quantity", 1, "expectedVersion", cart.get("version").asLong());
        request("POST", path + "/items", "22222222-2222-2222-2222-222222222222", add, 404);
        var before = request("POST", path + "/items", CUSTOMER, add, 200);
        var next = Map.of("productId", productId, "quantity", 1, "expectedVersion", before.get("version").asLong());
        var catalog = SERVICE_PROCESSES.get("product-service");
        catalog.destroy();
        try {
            assertTrue(catalog.waitFor(15, TimeUnit.SECONDS), "Catalog must stop before the outage assertion");
            request("POST", path + "/items", CUSTOMER, next, 503);
            assertEquals(before, request("GET", path, CUSTOMER, null, 200));
        } finally {
            if (catalog.isAlive()) { catalog.destroyForcibly(); assertTrue(catalog.waitFor(5, TimeUnit.SECONDS)); }
            startService("product-service", "product");
        }
        var recovered = request("POST", path + "/items", CUSTOMER, next, 200);
        assertEquals(2, recovered.get("items").get(0).get("quantity").asInt());
        assertEquals(before.get("version").asLong() + 1, recovered.get("version").asLong());
        request("POST", path + "/items", CUSTOMER, next, 409);
        assertEquals(recovered, request("GET", path, CUSTOMER, null, 200));
    }

    @Test void concurrentGatewayRetriesCreateOneOrderAndOneCharge() throws Exception {
        var order = new Order(UUID.randomUUID(), UUID.randomUUID()); var key = UUID.randomUUID();
        request("POST", "/api/inventory/stock", "admin", Map.of("productId", order.product(), "onHand", 1), 201);
        var command = orderCommand(order.id(), order.product(), "tok_success", "US", "12345");
        var ready = new java.util.concurrent.CountDownLatch(6); var go = new java.util.concurrent.CountDownLatch(1);
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var results = new ArrayList<java.util.concurrent.Future<JsonNode>>();
            for (int i = 0; i < 6; i++) results.add(executor.submit(() -> {
                ready.countDown(); assertTrue(go.await(5, TimeUnit.SECONDS));
                return request("POST", "/api/orders", "checkout", command, 202, key);
            }));
            try { assertTrue(ready.await(5, TimeUnit.SECONDS)); } finally { go.countDown(); }
            JsonNode first = results.getFirst().get(15, TimeUnit.SECONDS);
            for (var result : results) assertEquals(first, result.get(15, TimeUnit.SECONDS));
        }
        awaitOrder(order, "COMPLETED"); awaitNotification(order, "COMPLETED");
        assertEquals(1, scalar("order", "SELECT count(*) FROM order_command WHERE order_id = ?", order.id()));
        assertEquals(1, scalar("order", "SELECT count(*) FROM domain_event WHERE aggregate_id = ? AND event_type = 'OrderCreated'", order.id()));
        assertEquals(1, scalar("payment", "SELECT charge_count FROM provider_charge WHERE payment_id = ?", order.id()));
        var changed = new HashMap<>(command); changed.put("cartVersion", 2);
        request("POST", "/api/orders", "checkout", changed, 409, key);
        assertEquals(1, scalar("payment", "SELECT charge_count FROM provider_charge WHERE payment_id = ?", order.id()));
    }

    @Test void concurrentOrdersForLastUnitProduceOneCompletionAndOneRejection() throws Exception {
        var product = UUID.randomUUID(); var a = new Order(UUID.randomUUID(), product);
        UUID second;
        do { second = UUID.randomUUID(); } while (partition(a.id()) == partition(second));
        var b = new Order(second, product);
        request("POST", "/api/inventory/stock", "admin", Map.of("productId", product, "onHand", 1), 201);
        var ready = new java.util.concurrent.CountDownLatch(2); var go = new java.util.concurrent.CountDownLatch(1);
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var results = new ArrayList<java.util.concurrent.Future<JsonNode>>();
            for (var order : List.of(a, b)) results.add(executor.submit(() -> {
                ready.countDown(); assertTrue(go.await(5, TimeUnit.SECONDS));
                return request("POST", "/api/orders", "checkout", orderCommand(order.id(), product, "tok_success", "US", "12345"), 202);
            }));
            try { assertTrue(ready.await(5, TimeUnit.SECONDS)); } finally { go.countDown(); }
            for (var result : results) result.get(15, TimeUnit.SECONDS);
        }
        var outcomes = new ArrayList<String>(); long charges = 0;
        for (var order : List.of(a, b)) {
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos(); String outcome;
            do {
                outcome = request("GET", "/api/orders/" + order.id(), CUSTOMER, null, 200).get("status").asString();
                if (Set.of("COMPLETED", "CANCELLED").contains(outcome)) break;
                assertTrue(System.nanoTime() < deadline, "Competing order must finish"); Thread.sleep(100);
            } while (true);
            outcomes.add(outcome); awaitNotification(order, outcome); awaitView(order, outcome);
            charges += scalar("payment", "SELECT coalesce(sum(charge_count), 0) FROM provider_charge WHERE payment_id = ?", order.id());
        }
        assertEquals(1, Collections.frequency(outcomes, "COMPLETED")); assertEquals(1, Collections.frequency(outcomes, "CANCELLED"));
        assertEquals(1, charges);
        var stock = request("GET", "/api/inventory/stock/" + product, "admin", null, 200);
        assertEquals(1, stock.get("reserved").asInt()); assertEquals(0, stock.get("available").asInt());
    }
    static int partition(UUID id) {
        return org.apache.kafka.common.utils.Utils.toPositive(org.apache.kafka.common.utils.Utils.murmur2(id.toString().getBytes(StandardCharsets.UTF_8))) % 3;
    }
}
