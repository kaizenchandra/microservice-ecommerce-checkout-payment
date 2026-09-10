package com.synechisveltiosi.orderqueryservice;

import com.synechisveltiosi.orderqueryservice.application.ApiException;
import com.synechisveltiosi.orderqueryservice.application.OrderQueries;
import com.synechisveltiosi.orderqueryservice.application.ProjectionTransactions;
import com.synechisveltiosi.orderqueryservice.infrastructure.ProjectionCodec;
import com.synechisveltiosi.orderqueryservice.infrastructure.ProjectionRebuildWorker;
import com.synechisveltiosi.platform.contracts.EventEnvelope;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ProjectionApiIT {
    static final PostgreSQLContainer DB = new PostgreSQLContainer("postgres:17.6-alpine");
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1");
    static final UUID CUSTOMER = UUID.fromString("11111111-1111-1111-1111-111111111111");
    static final UUID OTHER = UUID.fromString("22222222-2222-2222-2222-222222222222");
    static final String PASSWORD = "projection-test-only";
    static ConfigurableApplicationContext context;
    static JdbcTemplate jdbc;
    static ProjectionTransactions projection;
    static ProjectionCodec codec;
    static OrderQueries queries;
    static TransactionTemplate tx;
    static String base;

    @BeforeAll
    static void start() {
        DB.start();
        KAFKA.start();
        startContext(false);
    }

    static void startContext(boolean rebuild) {
        context = SpringApplication.run(OrderQueryServiceApplication.class, "--server.port=0", "--projection.rebuild.enabled=" + rebuild,
                "--spring.datasource.url=" + DB.getJdbcUrl(), "--spring.datasource.username=" + DB.getUsername(), "--spring.datasource.password=" + DB.getPassword(),
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(), "--demo.auth.customer-password=" + PASSWORD,
                "--demo.auth.second-customer-password=" + PASSWORD, "--demo.auth.admin-password=" + PASSWORD);
        jdbc = context.getBean(JdbcTemplate.class);
        projection = context.getBean(ProjectionTransactions.class);
        codec = context.getBean(ProjectionCodec.class);
        queries = context.getBean(OrderQueries.class);
        tx = new TransactionTemplate(context.getBean(PlatformTransactionManager.class));
        base = "http://localhost:" + context.getEnvironment().getProperty("local.server.port");
    }

    @AfterAll
    static void stop() {
        if (context != null) context.close();
        KAFKA.stop();
        DB.stop();
    }

    static EventEnvelope<Map<String, Object>> event(UUID id, String type, long version, Map<String, Object> payload) {
        String aggregate = type.startsWith("Order") ? "Order" : type.startsWith("Inventory") ? "InventoryReservation" : type.startsWith("Payment") ? "Payment" : type.startsWith("Shipment") ? "Shipment" : "Notification";
        return new EventEnvelope<>(UUID.randomUUID(), type, UUID.randomUUID(), UUID.randomUUID(), aggregate, id, version,
                Instant.parse("2026-09-09T00:00:00Z").plusSeconds(version), 1, null, payload);
    }

    static EventEnvelope<Map<String, Object>> created(UUID id, UUID customer) {
        return event(id, "OrderCreated", 1, Map.of("orderId", id, "customerId", customer, "paymentToken", "tok_success",
                "items", List.of(Map.of("productId", UUID.randomUUID(), "sku", "SKU-1", "name", "Demo", "quantity", 2, "unitPrice", Map.of("amount", "12.50", "currency", "USD"))),
                "total", Map.of("amount", "25.00", "currency", "USD"), "shippingAddress", Map.of("recipient", "Demo", "line1", "Street", "city", "City", "postalCode", "12345", "country", "US")));
    }

    static String topic(EventEnvelope<?> event) {
        return switch (event.aggregateType()) {
            case "Order" -> "order.events";
            case "InventoryReservation" -> "inventory.events";
            case "Payment" -> "payment.events";
            case "Shipment" -> "shipping.events";
            default -> "notification.events";
        };
    }

    static void accept(EventEnvelope<?> event) {
        projection.accept(codec.sanitize(topic(event), event.aggregateId().toString(), codec.encode(event)));
    }

    static void rebuildAll() {
        for (int i = 0; i < 1000 && projection.rebuildStep(10); i++) {
        }
        assertNull(projection.buildingGeneration());
    }

    static HttpResponse<String> request(String method, String path, String user) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(10));
        if (user != null)
            builder.header("Authorization", "Basic " + Base64.getEncoder().encodeToString((user + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8)));
        return HttpClient.newHttpClient().send(builder.method(method, HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void buffersGapsAndIndependentProducerStreamsUntilMissingVersionsArrive() {
        var id = UUID.randomUUID();
        var identity = Map.<String, Object>of("orderId", id, "customerId", CUSTOMER);
        accept(event(id, "PaymentRefunded", 2, Map.of("orderId", id, "customerId", CUSTOMER, "refundReference", UUID.randomUUID())));
        accept(event(id, "InventoryReleased", 2, identity));
        accept(event(id, "OrderCancelled", 7, identity));
        assertThrows(ApiException.class, () -> queries.get(id, CUSTOMER, false));
        accept(created(id, CUSTOMER));
        var initial = queries.get(id, CUSTOMER, false);
        assertEquals("PENDING", initial.status());
        assertEquals(1, initial.orderVersion());
        assertEquals("UNKNOWN", initial.paymentStatus());
        assertTrue(projection.status().bufferedEvents() >= 3);
        accept(event(id, "PaymentCompleted", 1, Map.of("orderId", id, "customerId", CUSTOMER, "providerReference", UUID.randomUUID())));
        accept(event(id, "InventoryReserved", 1, identity));
        var intermediate = queries.get(id, CUSTOMER, false);
        assertEquals("REFUNDED", intermediate.refundStatus());
        assertEquals("RELEASED", intermediate.inventoryStatus());
        int version = 2;
        for (String fact : List.of("InventoryReserved", "PaymentCompleted", "ShipmentFailed", "PaymentRefunded", "InventoryReleased"))
            accept(event(id, "OrderFactRecorded", version++, Map.of("fact", fact)));
        var done = queries.get(id, CUSTOMER, false);
        assertEquals("CANCELLED", done.status());
        assertEquals(7, done.orderVersion());
        assertEquals(2L, done.sourceVersions().get("Payment"));
        assertEquals(2L, done.sourceVersions().get("InventoryReservation"));
    }

    @Test
    void deduplicatesAliasesAndRejectsChangedIdentityWithoutLeakingTokens() throws Exception {
        var id = UUID.randomUUID();
        var original = created(id, CUSTOMER);
        accept(original);
        accept(original);
        var alias = new EventEnvelope<>(UUID.randomUUID(), original.eventType(), original.correlationId(), original.causationId(), original.aggregateType(), id,
                1, original.occurredAt(), 1, null, original.payload());
        accept(alias);
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM projection_event WHERE order_id = ?", Integer.class, id));
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM projection_inbox WHERE event_sequence IN (SELECT sequence FROM projection_event WHERE order_id = ?)", Integer.class, id));
        assertFalse(jdbc.queryForObject("SELECT envelope::text FROM projection_event WHERE order_id = ?", String.class, id).contains("paymentToken"));
        assertFalse(request("GET", "/api/order-views/" + id, CUSTOMER.toString()).body().contains("tok_success"));
        var modified = new HashMap<>(original.payload());
        modified.put("customerId", OTHER);
        var conflict = event(id, "OrderCreated", 1, modified);
        assertThrows(IllegalArgumentException.class, () -> accept(conflict));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM projection_inbox WHERE event_id = ?", Integer.class, conflict.eventId()));
        assertThrows(org.springframework.dao.DataAccessException.class, () -> jdbc.update("DELETE FROM projection_event WHERE order_id = ?", id));
    }

    @Test
    void apiOwnershipPaginationAndRebuildPermissions() throws Exception {
        var id = UUID.randomUUID();
        accept(created(id, CUSTOMER));
        var other = UUID.randomUUID();
        accept(created(other, OTHER));
        String path = "/api/order-views/" + id;
        assertEquals(401, request("GET", path, null).statusCode());
        assertEquals(404, request("GET", path, OTHER.toString()).statusCode());
        assertEquals(200, request("GET", path, "admin").statusCode());
        assertEquals(403, request("POST", "/api/order-views/admin/rebuild", CUSTOMER.toString()).statusCode());
        assertEquals(403, request("GET", "/api/order-views/admin/projection", CUSTOMER.toString()).statusCode());
        assertEquals(400, request("GET", "/api/order-views?size=101", CUSTOMER.toString()).statusCode());
        assertEquals(400, request("GET", "/api/order-views?status=BOGUS", CUSTOMER.toString()).statusCode());
        var page = codec.read(request("GET", "/api/order-views?size=100&customerId=" + OTHER, CUSTOMER.toString()).body(), OrderQueries.Page.class);
        assertTrue(page.items().stream().allMatch(view -> view.customerId().equals(CUSTOMER)));
        assertTrue(page.items().stream().noneMatch(view -> view.orderId().equals(other)));
        assertEquals(202, request("POST", "/api/order-views/admin/rebuild", "admin").statusCode());
        assertEquals(409, request("POST", "/api/order-views/admin/rebuild", "admin").statusCode());
        rebuildAll();
    }

    @Test
    void rebuildKeepsActiveReadsAndCatchesConcurrentLiveInputBeforeSwitch() throws Exception {
        var id = UUID.randomUUID();
        accept(created(id, CUSTOMER));
        long old = projection.status().activeGeneration();
        long building = projection.startRebuild().generation();
        assertTrue(projection.rebuildStep(1));
        assertEquals(old, projection.status().activeGeneration());
        var note = event(id, "OrderNoteAdded", 2, Map.of("customerId", CUSTOMER, "note", "Arrived during rebuild"));
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var barrier = new CyclicBarrier(2);
            var a = executor.submit(() -> {
                barrier.await(10, TimeUnit.SECONDS);
                accept(note);
                return true;
            });
            var b = executor.submit(() -> {
                barrier.await(10, TimeUnit.SECONDS);
                return projection.rebuildStep(1);
            });
            a.get(15, TimeUnit.SECONDS);
            b.get(15, TimeUnit.SECONDS);
        }
        var live = queries.get(id, CUSTOMER, false);
        assertEquals(2, live.orderVersion());
        rebuildAll();
        assertEquals(building, projection.status().activeGeneration());
        assertEquals(live, queries.get(id, CUSTOMER, false));
        assertEquals("RETIRED", jdbc.queryForObject("SELECT state FROM projection_generation WHERE id = ?", String.class, old));
    }

    @Test
    void failedRebuildLeavesActiveGenerationReadableAndCanBeReplaced() {
        var id = UUID.randomUUID();
        accept(created(id, CUSTOMER));
        long active = projection.status().activeGeneration();
        long failed = projection.startRebuild().generation();
        jdbc.execute("ALTER TABLE order_projection ADD CONSTRAINT test_rebuild_failure CHECK (generation <> " + failed + ") NOT VALID");
        try {
            new ProjectionRebuildWorker(projection, 100).poll();
        } finally {
            jdbc.execute("ALTER TABLE order_projection DROP CONSTRAINT test_rebuild_failure");
        }
        assertEquals(active, projection.status().activeGeneration());
        assertNull(projection.buildingGeneration());
        assertEquals("FAILED", jdbc.queryForObject("SELECT state FROM projection_generation WHERE id = ?", String.class, failed));
        assertEquals(id, queries.get(id, CUSTOMER, false).orderId());
        long next = projection.startRebuild().generation();
        rebuildAll();
        assertEquals(next, projection.status().activeGeneration());
    }

    @Test
    void inputAndProjectionCommitAtomicallyAndRejectInvalidEnvelope() {
        var id = UUID.randomUUID();
        var created = created(id, CUSTOMER);
        assertThrows(IllegalStateException.class, () -> tx.executeWithoutResult(status -> {
            accept(created);
            throw new IllegalStateException("Crash before commit");
        }));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM projection_event WHERE order_id = ?", Integer.class, id));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM projection_inbox WHERE event_id = ?", Integer.class, created.eventId()));
        assertThrows(ApiException.class, () -> queries.get(id, CUSTOMER, false));
        assertThrows(IllegalArgumentException.class, () -> codec.sanitize("payment.events", id.toString(), codec.encode(created)));
        assertThrows(RuntimeException.class, () -> codec.sanitize("order.events", id.toString(), codec.encode(created).replace("\"schemaVersion\":1", "\"schemaVersion\":1.5")));
        accept(created);
        assertEquals(1, queries.get(id, CUSTOMER, false).orderVersion());
    }

    @Test
    void realKafkaConsumerProjectsAndRestartResumesIncompleteRebuild() throws Exception {
        var id = UUID.randomUUID();
        var event = created(id, CUSTOMER);
        // Query has no business producer; the test supplies a serializer explicitly for this fixture.
        var props = new Properties();
        props.put("bootstrap.servers", KAFKA.getBootstrapServers());
        try (var producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props,
                new org.apache.kafka.common.serialization.StringSerializer(), new org.apache.kafka.common.serialization.StringSerializer())) {
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>("order.events", id.toString(), codec.encode(event))).get(10, TimeUnit.SECONDS);
        }
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (jdbc.queryForObject("SELECT count(*) FROM projection_event WHERE order_id = ?", Integer.class, id) == 0 && System.nanoTime() < deadline)
            Thread.sleep(50);
        var before = queries.get(id, CUSTOMER, false);
        long build = projection.startRebuild().generation();
        projection.rebuildStep(1);
        context.close();
        startContext(true);
        try {
            deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (projection.buildingGeneration() != null && System.nanoTime() < deadline) Thread.sleep(100);
            assertEquals(build, projection.status().activeGeneration());
            assertEquals(before, queries.get(id, CUSTOMER, false));
        } finally {
            context.close();
            startContext(false);
        }
    }
}
