package com.synechisveltiosi.inventoryservice;

import com.synechisveltiosi.inventoryservice.api.InventoryDtos;
import com.synechisveltiosi.inventoryservice.application.ApiException;
import com.synechisveltiosi.inventoryservice.application.CompensationTransactions;
import com.synechisveltiosi.inventoryservice.application.InventoryService;
import com.synechisveltiosi.inventoryservice.application.InventoryTransactions;
import com.synechisveltiosi.inventoryservice.domain.InventoryEvents;
import com.synechisveltiosi.inventoryservice.infrastructure.OrderEventListener;
import com.synechisveltiosi.inventoryservice.infrastructure.OutboxPublisher;
import com.synechisveltiosi.inventoryservice.infrastructure.StockRepository;
import com.synechisveltiosi.platform.contracts.EventEnvelope;
import com.synechisveltiosi.platform.contracts.Money;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class InventoryApiIT {
    static final PostgreSQLContainer DB = new PostgreSQLContainer("postgres:17.6-alpine");
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1");
    static final UUID CUSTOMER = UUID.fromString("11111111-1111-1111-1111-111111111111");
    static final String PASSWORD = "integration-test-only";
    static ConfigurableApplicationContext context;
    static InventoryService inventory;
    static InventoryTransactions transactions;
    static JdbcTemplate jdbc;
    static JsonMapper json;
    static TransactionTemplate tx;
    static String base;

    @BeforeAll
    static void start() {
        DB.start();
        KAFKA.start();
        context = SpringApplication.run(InventoryServiceApplication.class, "--server.port=0", "--outbox.enabled=false", "--compensation.enabled=false", "--spring.profiles.active=demo",
                "--spring.datasource.url=" + DB.getJdbcUrl(), "--spring.datasource.username=" + DB.getUsername(),
                "--spring.datasource.password=" + DB.getPassword(), "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--demo.auth.customer-password=" + PASSWORD, "--demo.auth.second-customer-password=" + PASSWORD, "--demo.auth.admin-password=" + PASSWORD);
        inventory = context.getBean(InventoryService.class);
        transactions = context.getBean(InventoryTransactions.class);
        jdbc = context.getBean(JdbcTemplate.class);
        json = context.getBean(JsonMapper.class);
        tx = new TransactionTemplate(context.getBean(PlatformTransactionManager.class));
        base = "http://localhost:" + context.getEnvironment().getProperty("local.server.port");
    }

    @AfterAll
    static void stop() {
        if (context != null) context.close();
        KAFKA.stop();
        DB.stop();
    }

    static UUID stock(int count) {
        var id = UUID.randomUUID();
        transactions.createStock(new InventoryDtos.CreateStock(id, count));
        return id;
    }

    static EventEnvelope<InventoryEvents.OrderCreated> event(UUID product, int quantity) {
        return event(List.of(new InventoryEvents.Line(product, quantity)));
    }

    static EventEnvelope<InventoryEvents.OrderCreated> event(List<InventoryEvents.Line> lines) {
        var id = UUID.randomUUID();
        var payload = new InventoryEvents.OrderCreated(id, CUSTOMER, lines, new Money(new BigDecimal("25.00"), Currency.getInstance("USD")),
                "tok_success", new InventoryEvents.Address("Demo Buyer", "1 Test Street", "Test City", "12345", "US"));
        return new EventEnvelope<>(UUID.randomUUID(), "OrderCreated", UUID.randomUUID(), UUID.randomUUID(), "Order", id, 1, Instant.now(), 1,
                "00-12345678901234567890123456789012-1234567890123456-01", payload);
    }

    static EventEnvelope<InventoryEvents.OrderCreated> redelivery(EventEnvelope<InventoryEvents.OrderCreated> e) {
        return new EventEnvelope<>(UUID.randomUUID(), e.eventType(), e.correlationId(), e.causationId(), e.aggregateType(), e.aggregateId(), e.aggregateVersion(),
                e.occurredAt(), e.schemaVersion(), e.traceparent(), e.payload());
    }

    static HttpResponse<String> request(String method, String path, String user, Object body, UUID key) throws Exception {
        var request = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(10)).header("Content-Type", "application/json");
        if (user != null)
            request.header("Authorization", "Basic " + Base64.getEncoder().encodeToString((user + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8)));
        if (key != null) request.header("Idempotency-Key", key.toString());
        return HttpClient.newHttpClient().send(request.method(method, body == null ? HttpRequest.BodyPublishers.noBody() :
                HttpRequest.BodyPublishers.ofString(body instanceof String s ? s : json.writeValueAsString(body))).build(), HttpResponse.BodyHandlers.ofString());
    }

    static int count(String table, String column, UUID id) {
        return jdbc.queryForObject("SELECT count(*) FROM " + table + " WHERE " + column + " = ?", Integer.class, id);
    }

    @Test
    void securedStockApiValidatesVersionsAndPreservesReservations() throws Exception {
        String path = "/api/inventory/stock";
        var command = new InventoryDtos.CreateStock(UUID.randomUUID(), 5);
        assertEquals(401, request("POST", path, null, command, null).statusCode());
        assertEquals(403, request("POST", path, CUSTOMER.toString(), command, null).statusCode());
        var response = request("POST", path, "admin", command, null);
        assertEquals(201, response.statusCode(), response.body());
        assertEquals(409, request("POST", path, "admin", command, null).statusCode());
        path += "/" + command.productId();
        assertEquals(400, request("PUT", path, "admin", "{\"onHand\":1.5,\"expectedVersion\":0}", null).statusCode());
        assertEquals(200, request("PUT", path, "admin", new InventoryDtos.SetStock(6, 0L), null).statusCode());
        assertEquals(409, request("PUT", path, "admin", new InventoryDtos.SetStock(7, 0L), null).statusCode());
        var event = event(command.productId(), 4);
        inventory.reserve(event);
        var stock = transactions.getStock(command.productId());
        assertEquals(409, request("PUT", path, "admin", new InventoryDtos.SetStock(3, stock.version()), null).statusCode());
        var reservationPath = "/api/inventory/reservations/" + event.aggregateId();
        assertEquals(403, request("GET", reservationPath, CUSTOMER.toString(), null, null).statusCode());
        assertEquals(200, request("GET", reservationPath, "admin", null, null).statusCode());
        var key = UUID.randomUUID();
        assertEquals(200, request("POST", reservationPath + "/release", "admin", null, key).statusCode());
        assertEquals(200, request("POST", reservationPath + "/release", "admin", null, key).statusCode());
        assertEquals(6, transactions.getStock(command.productId()).available());
        assertEquals(10, transactions.getStock(UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1")).available());
    }

    @Test
    void allOrNothingAndSemanticDeduplication() {
        var first = stock(5);
        var scarce = stock(0);
        var rejected = event(List.of(new InventoryEvents.Line(first, 3), new InventoryEvents.Line(scarce, 1)));
        inventory.reserve(rejected);
        assertEquals("REJECTED", transactions.reservation(rejected.aggregateId()).status());
        assertEquals(0, transactions.getStock(first).reserved());
        var accepted = event(first, 2);
        inventory.reserve(accepted);
        inventory.reserve(accepted);
        inventory.reserve(redelivery(accepted));
        assertEquals(2, transactions.getStock(first).reserved());
        assertEquals(1, count("outbox_event", "aggregate_id", accepted.aggregateId()));
        inventory.release(accepted.aggregateId(), UUID.randomUUID(), UUID.randomUUID());
        inventory.release(accepted.aggregateId(), UUID.randomUUID(), UUID.randomUUID());
        inventory.reserve(redelivery(accepted));
        assertEquals(0, transactions.getStock(first).reserved());
        assertEquals("RELEASED", transactions.reservation(accepted.aggregateId()).status());
        assertEquals(2, count("outbox_event", "aggregate_id", accepted.aggregateId()));
        var missing = event(UUID.randomUUID(), 1);
        inventory.reserve(missing);
        assertEquals("STOCK_NOT_FOUND", transactions.reservation(missing.aggregateId()).reason());
        assertThrows(ApiException.class, () -> inventory.release(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()));
    }

    @Test
    void lastUnitAndDuplicateOrdersUnderConcurrency() throws Exception {
        var product = stock(1);
        var first = event(product, 1);
        var second = event(product, 1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var barrier = new CyclicBarrier(2);
            var a = executor.submit(() -> {
                barrier.await(10, TimeUnit.SECONDS);
                inventory.reserve(first);
                return true;
            });
            var b = executor.submit(() -> {
                barrier.await(10, TimeUnit.SECONDS);
                inventory.reserve(second);
                return true;
            });
            a.get(15, TimeUnit.SECONDS);
            b.get(15, TimeUnit.SECONDS);
            assertEquals(Set.of("RESERVED", "REJECTED"), Set.of(transactions.reservation(first.aggregateId()).status(), transactions.reservation(second.aggregateId()).status()));
            assertEquals(1, transactions.getStock(product).reserved());
            var duplicate = event(stock(10), 3);
            var c = executor.submit(() -> {
                barrier.await(10, TimeUnit.SECONDS);
                inventory.reserve(duplicate);
                return true;
            });
            var d = executor.submit(() -> {
                barrier.await(10, TimeUnit.SECONDS);
                inventory.reserve(redelivery(duplicate));
                return true;
            });
            c.get(15, TimeUnit.SECONDS);
            d.get(15, TimeUnit.SECONDS);
            assertEquals(3, transactions.getStock(duplicate.payload().items().getFirst().productId()).reserved());
            assertEquals(1, count("outbox_event", "aggregate_id", duplicate.aggregateId()));
            var releaseA = executor.submit(() -> inventory.release(duplicate.aggregateId(), UUID.randomUUID(), UUID.randomUUID()));
            var releaseB = executor.submit(() -> inventory.release(duplicate.aggregateId(), UUID.randomUUID(), UUID.randomUUID()));
            assertEquals("RELEASED", releaseA.get(15, TimeUnit.SECONDS).status());
            assertEquals("RELEASED", releaseB.get(15, TimeUnit.SECONDS).status());
            assertEquals(0, transactions.getStock(duplicate.payload().items().getFirst().productId()).reserved());
            assertEquals(2, count("outbox_event", "aggregate_id", duplicate.aggregateId()));
        }
    }

    @Test
    void versionConflictRollsBackAndOutboxFailureRollsBackWholeReservation() throws Exception {
        var product = stock(5);
        var repository = context.getBean(StockRepository.class);
        var barrier = new CyclicBarrier(2);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Callable<Boolean> change = () -> {
                try {
                    tx.executeWithoutResult(status -> {
                        var loaded = repository.findById(product).orElseThrow();
                        try {
                            barrier.await(10, TimeUnit.SECONDS);
                        } catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                        loaded.reserve(1);
                        repository.flush();
                    });
                    return true;
                } catch (org.springframework.dao.OptimisticLockingFailureException conflict) {
                    return false;
                }
            };
            var a = executor.submit(change);
            var b = executor.submit(change);
            assertNotEquals(a.get(15, TimeUnit.SECONDS), b.get(15, TimeUnit.SECONDS));
        }
        assertEquals(1, transactions.getStock(product).reserved());
        var event = event(stock(5), 3);
        // Fault injection after stock flush and before commit, limited to this isolated database.
        jdbc.execute("ALTER TABLE outbox_event ADD CONSTRAINT test_failure CHECK (aggregate_id <> '" + event.aggregateId() + "'::uuid) NOT VALID");
        try {
            assertThrows(org.springframework.dao.DataIntegrityViolationException.class, () -> inventory.reserve(event));
        } finally {
            jdbc.execute("ALTER TABLE outbox_event DROP CONSTRAINT test_failure");
        }
        assertEquals(0, transactions.getStock(event.payload().items().getFirst().productId()).reserved());
        assertEquals(0, count("reservation", "order_id", event.aggregateId()));
        assertEquals(0, count("processed_event", "event_id", event.eventId()));
        assertEquals(0, count("outbox_event", "aggregate_id", event.aggregateId()));
        inventory.reserve(event);
        assertEquals("RESERVED", transactions.reservation(event.aggregateId()).status());
    }

    @Test
    @SuppressWarnings("unchecked")
    void realOrderConsumerAndInventoryOutboxPreserveOrderingAndMetadata() throws Exception {
        var event = event(stock(10), 2);
        KafkaTemplate<String, String> producer = context.getBean(KafkaTemplate.class);
        // This is a consumer-owned mapping; extra producer fields are accepted.
        var tree = (tools.jackson.databind.node.ObjectNode) json.readTree(json.writeValueAsString(event));
        ((tools.jackson.databind.node.ObjectNode) tree.get("payload")).put("salesChannel", "WEB");
        producer.send("order.events", event.aggregateId().toString(), json.writeValueAsString(tree)).get(10, TimeUnit.SECONDS);
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (count("reservation", "order_id", event.aggregateId()) == 0 && System.nanoTime() < deadline)
            Thread.sleep(50);
        assertEquals("RESERVED", transactions.reservation(event.aggregateId()).status());
        inventory.release(event.aggregateId(), UUID.randomUUID(), event.correlationId());
        var publisher = context.getBean(OutboxPublisher.class);
        for (int i = 0; i < 100 && publisher.publishOne(); i++) {
        }
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        try (var consumer = new KafkaConsumer<String, String>(props, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of("inventory.events"));
            var records = new ArrayList<ConsumerRecord<String, String>>();
            deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (records.size() < 2 && System.nanoTime() < deadline)
                for (var record : consumer.poll(Duration.ofMillis(200)))
                    if (event.aggregateId().toString().equals(record.key())) records.add(record);
            assertEquals(2, records.size());
            var first = json.readTree(records.getFirst().value());
            var second = json.readTree(records.getLast().value());
            assertEquals("InventoryReserved", first.get("eventType").asString());
            assertEquals("InventoryReleased", second.get("eventType").asString());
            assertEquals(1, first.get("aggregateVersion").asLong());
            assertEquals(2, second.get("aggregateVersion").asLong());
            assertEquals(event.eventId().toString(), first.get("causationId").asString());
            assertEquals(event.correlationId().toString(), first.get("correlationId").asString());
            assertEquals(event.traceparent(), new String(records.getFirst().headers().lastHeader("traceparent").value(), StandardCharsets.UTF_8));
            assertEquals(new BigDecimal("25.00"), first.get("payload").get("total").get("amount").decimalValue().setScale(2));
            assertTrue(publisher.deliveries(event.aggregateId()).stream().allMatch(row -> row.status().equals("PUBLISHED")));
        }
    }

    @Test
    void invalidIdentityAndSchemaNeverMarkProcessed() {
        var event = event(stock(2), 1);
        var listener = context.getBean(OrderEventListener.class);
        assertThrows(IllegalArgumentException.class, () -> listener.receive(new ConsumerRecord<>("order.events", 0, 0, "wrong-key", json.writeValueAsString(event))));
        String future = json.writeValueAsString(event).replace("\"schemaVersion\":1", "\"schemaVersion\":2");
        assertThrows(IllegalArgumentException.class, () -> listener.receive(new ConsumerRecord<>("order.events", 0, 0, event.aggregateId().toString(), future)));
        String fractional = json.writeValueAsString(event).replace("\"schemaVersion\":1", "\"schemaVersion\":1.5");
        assertThrows(RuntimeException.class, () -> listener.receive(new ConsumerRecord<>("order.events", 0, 0, event.aggregateId().toString(), fractional)));
        assertEquals(0, count("processed_event", "event_id", event.eventId()));
        inventory.reserve(event);
        var changed = new InventoryEvents.OrderCreated(event.aggregateId(), CUSTOMER, List.of(new InventoryEvents.Line(event.payload().items().getFirst().productId(), 2)),
                event.payload().total(), event.payload().paymentToken(), event.payload().shippingAddress());
        var conflict = new EventEnvelope<>(UUID.randomUUID(), event.eventType(), event.correlationId(), event.causationId(), "Order", event.aggregateId(), 1, event.occurredAt(), 1, null, changed);
        assertThrows(ApiException.class, () -> inventory.reserve(conflict));
        assertEquals(0, count("processed_event", "event_id", conflict.eventId()));
        assertEquals(1, transactions.getStock(event.payload().items().getFirst().productId()).reserved());
    }

    @Test
    @SuppressWarnings("unchecked")
    void failedPublicationBlocksSuccessorAndRollbackKeepsStableEventIdentity() {
        var real = context.getBean(OutboxPublisher.class);
        for (int i = 0; i < 100 && real.publishOne(); i++) {
        }
        var event = event(stock(4), 2);
        inventory.reserve(event);
        inventory.release(event.aggregateId(), UUID.randomUUID(), event.correlationId());
        KafkaTemplate<String, String> kafka = org.mockito.Mockito.mock(KafkaTemplate.class);
        org.mockito.Mockito.when(kafka.send(org.mockito.ArgumentMatchers.any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("Broker unavailable")));
        var publisher = new OutboxPublisher(jdbc, kafka, json, context.getBean(io.micrometer.core.instrument.MeterRegistry.class), context.getBean(com.synechisveltiosi.inventoryservice.infrastructure.Telemetry.class));
        assertEquals(Boolean.TRUE, tx.execute(status -> publisher.publishOne()));
        assertEquals(Boolean.FALSE, tx.execute(status -> publisher.publishOne()));
        assertEquals(0, publisher.deliveries(event.aggregateId()).getLast().attempts());
        jdbc.update("UPDATE outbox_event SET next_attempt_at = now() WHERE aggregate_id = ?", event.aggregateId());
        var sent = new ArrayList<String>();
        org.mockito.Mockito.when(kafka.send(org.mockito.ArgumentMatchers.any(ProducerRecord.class))).thenAnswer(invocation -> {
            ProducerRecord<String, String> record = invocation.getArgument(0);
            sent.add(record.value());
            return CompletableFuture.completedFuture(null);
        });
        tx.executeWithoutResult(status -> {
            assertTrue(publisher.publishOne());
            status.setRollbackOnly();
        });
        assertEquals("PENDING", publisher.deliveries(event.aggregateId()).getFirst().status());
        tx.executeWithoutResult(status -> assertTrue(publisher.publishOne()));
        assertEquals(sent.get(0), sent.get(1));
        tx.executeWithoutResult(status -> assertTrue(publisher.publishOne()));
        assertEquals(2, json.readTree(sent.getLast()).get("aggregateVersion").asLong());
    }

    @Test
    void compensationArrivingBeforeReservationIsRetainedAndReleasedOnce() {
        var event = event(stock(5), 2);
        var compensation = context.getBean(CompensationTransactions.class);
        var failed = new EventEnvelope<>(UUID.randomUUID(), "PaymentFailed", event.correlationId(), event.eventId(), "Payment", event.aggregateId(), 1,
                java.time.Instant.now(), 1, event.traceparent(), new CompensationTransactions.Payload(event.aggregateId(), CUSTOMER));
        compensation.accept(failed);
        assertFalse(compensation.processOne());
        inventory.reserve(event);
        assertTrue(compensation.processOne());
        compensation.accept(failed);
        assertFalse(compensation.processOne());
        assertEquals("RELEASED", transactions.reservation(event.aggregateId()).status());
        assertEquals(0, transactions.getStock(event.payload().items().getFirst().productId()).reserved());
        assertEquals(2, count("outbox_event", "aggregate_id", event.aggregateId()));
    }
}
