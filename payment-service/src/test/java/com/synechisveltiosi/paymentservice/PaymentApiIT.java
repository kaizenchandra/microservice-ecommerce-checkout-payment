package com.synechisveltiosi.paymentservice;

import com.synechisveltiosi.paymentservice.api.PaymentDtos;
import com.synechisveltiosi.paymentservice.application.*;
import com.synechisveltiosi.paymentservice.domain.*;
import com.synechisveltiosi.paymentservice.infrastructure.*;
import com.synechisveltiosi.platform.contracts.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class PaymentApiIT {
    static final PostgreSQLContainer DB = new PostgreSQLContainer("postgres:17.6-alpine");
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1");
    static final UUID CUSTOMER = UUID.fromString("11111111-1111-1111-1111-111111111111");
    static final String PASSWORD = "integration-test-only";
    static ConfigurableApplicationContext context;
    static PaymentTransactions payments;
    static PaymentWorker worker;
    static PaymentProvider provider;
    static ProviderLedger ledger;
    static JdbcTemplate jdbc;
    static JsonMapper json;
    static TransactionTemplate tx;
    static String base;
    @BeforeAll static void start() { DB.start(); KAFKA.start(); startContext(false); }
    static void startContext(boolean recovery) {
        context = SpringApplication.run(PaymentServiceApplication.class, "--server.port=0", "--outbox.enabled=false", "--payment.recovery.enabled=" + recovery,
                "--spring.datasource.url=" + DB.getJdbcUrl(), "--spring.datasource.username=" + DB.getUsername(),
                "--spring.datasource.password=" + DB.getPassword(), "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--demo.auth.customer-password=" + PASSWORD, "--demo.auth.second-customer-password=" + PASSWORD, "--demo.auth.admin-password=" + PASSWORD);
        payments = context.getBean(PaymentTransactions.class); worker = context.getBean(PaymentWorker.class);
        provider = context.getBean(PaymentProvider.class); ledger = context.getBean(ProviderLedger.class);
        jdbc = context.getBean(JdbcTemplate.class); json = context.getBean(JsonMapper.class);
        tx = new TransactionTemplate(context.getBean(PlatformTransactionManager.class));
        base = "http://localhost:" + context.getEnvironment().getProperty("local.server.port");
    }
    @AfterAll static void stop() { if (context != null) context.close(); KAFKA.stop(); DB.stop(); }
    @BeforeEach void drainPreviousWork() {
        for (int round = 0; round < 4; round++) {
            jdbc.update("UPDATE payment SET lease_token = NULL, lease_until = NULL, next_attempt_at = now() WHERE status IN ('PENDING', 'UNKNOWN')");
            for (int i = 0; i < 100 && worker.processOne(); i++) { }
        }
    }
    static EventEnvelope<PaymentEvents.InventoryReserved> event(String token) {
        var id = UUID.randomUUID();
        var payload = new PaymentEvents.InventoryReserved(id, CUSTOMER, List.of(new PaymentEvents.Line(UUID.randomUUID(), 2)),
                new Money(new BigDecimal("25.00"), Currency.getInstance("USD")), token,
                new PaymentEvents.Address("Demo Buyer", "1 Test Street", "Test City", "12345", "US"));
        return new EventEnvelope<>(UUID.randomUUID(), "InventoryReserved", UUID.randomUUID(), UUID.randomUUID(), "InventoryReservation", id,
                1, Instant.now(), 1, "00-12345678901234567890123456789012-1234567890123456-01", payload);
    }
    static EventEnvelope<PaymentEvents.InventoryReserved> redelivery(EventEnvelope<PaymentEvents.InventoryReserved> e) {
        return new EventEnvelope<>(UUID.randomUUID(), e.eventType(), e.correlationId(), e.causationId(), e.aggregateType(), e.aggregateId(), e.aggregateVersion(),
                e.occurredAt(), e.schemaVersion(), e.traceparent(), e.payload());
    }
    static PaymentProvider.Request charge(EventEnvelope<PaymentEvents.InventoryReserved> e) { return new PaymentProvider.Request(e.aggregateId(), e.payload().total(), e.payload().paymentToken()); }
    static PaymentDtos.View view(UUID id) { return payments.get(id, null, true); }
    static int count(String table, String column, UUID id) { return jdbc.queryForObject("SELECT count(*) FROM " + table + " WHERE " + column + " = ?", Integer.class, id); }
    static HttpResponse<String> request(String method, String path, String user) throws Exception {
        var request = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(10));
        if (user != null) request.header("Authorization", "Basic " + Base64.getEncoder().encodeToString((user + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8)));
        return HttpClient.newHttpClient().send(request.method(method, HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
    }
    @Test void successAndDeclineHaveOneDurableTerminalEvent() {
        for (String token : List.of("tok_success", "tok_declined")) {
            var event = event(token); payments.accept(event);
            assertEquals("PENDING", view(event.aggregateId()).status()); assertTrue(worker.processOne());
            assertEquals(token.equals("tok_success") ? "COMPLETED" : "FAILED", view(event.aggregateId()).status());
            assertEquals(token.equals("tok_success") ? 1 : 0, ledger.view(event.aggregateId()).orElseThrow().chargeCount());
            payments.accept(event); payments.accept(redelivery(event)); payments.retry(event.aggregateId());
            assertFalse(worker.processOne()); assertEquals(1, count("outbox_event", "aggregate_id", event.aggregateId()));
            assertEquals(1, view(event.aggregateId()).version());
        }
    }
    @Test void responseLossReconcilesWithoutChargingAgain() {
        var event = event("tok_timeout"); payments.accept(event); assertTrue(worker.processOne());
        assertEquals("UNKNOWN", view(event.aggregateId()).status());
        assertEquals(1, ledger.view(event.aggregateId()).orElseThrow().chargeCount());
        assertEquals(0, count("outbox_event", "aggregate_id", event.aggregateId()));
        payments.retry(event.aggregateId()); assertTrue(worker.processOne());
        assertEquals("COMPLETED", view(event.aggregateId()).status());
        assertEquals(1, ledger.view(event.aggregateId()).orElseThrow().requests());
        assertEquals(1, count("outbox_event", "aggregate_id", event.aggregateId()));
    }
    @Test void providerUnavailableIsUnknownUntilRecoveryAndNeverDeclined() {
        var event = event("tok_error"); payments.accept(event);
        for (int i = 0; i < 2; i++) {
            assertTrue(worker.processOne()); assertEquals("UNKNOWN", view(event.aggregateId()).status());
            assertEquals(0, ledger.view(event.aggregateId()).orElseThrow().chargeCount());
            assertEquals(0, count("outbox_event", "aggregate_id", event.aggregateId())); payments.retry(event.aggregateId());
        }
        assertTrue(worker.processOne()); assertEquals("COMPLETED", view(event.aggregateId()).status());
        assertEquals(3, ledger.view(event.aggregateId()).orElseThrow().requests());
        assertEquals(1, ledger.view(event.aggregateId()).orElseThrow().chargeCount());
    }
    @Test void concurrentAcceptsDeduplicateAndChangedInstructionsRollbackInbox() throws Exception {
        var event = event("tok_success");
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var barrier = new CyclicBarrier(2);
            var a = executor.submit(() -> { barrier.await(10, TimeUnit.SECONDS); payments.accept(event); return true; });
            var b = executor.submit(() -> { barrier.await(10, TimeUnit.SECONDS); payments.accept(redelivery(event)); return true; });
            a.get(15, TimeUnit.SECONDS); b.get(15, TimeUnit.SECONDS);
            var first = executor.submit(worker::processOne); var second = executor.submit(worker::processOne);
            assertNotEquals(first.get(15, TimeUnit.SECONDS), second.get(15, TimeUnit.SECONDS));
        }
        assertEquals(1, count("payment", "payment_id", event.aggregateId()));
        assertEquals(1, ledger.view(event.aggregateId()).orElseThrow().chargeCount());
        var old = event.payload();
        var changed = new PaymentEvents.InventoryReserved(old.orderId(), old.customerId(), old.items(), new Money(new BigDecimal("26"), Currency.getInstance("USD")), old.paymentToken(), old.shippingAddress());
        var conflict = new EventEnvelope<>(UUID.randomUUID(), event.eventType(), event.correlationId(), event.causationId(), event.aggregateType(), event.aggregateId(), 1, event.occurredAt(), 1, null, changed);
        assertThrows(ApiException.class, () -> payments.accept(conflict));
        assertEquals(0, count("processed_event", "event_id", conflict.eventId()));
        assertThrows(IllegalArgumentException.class, () -> provider.charge(new PaymentProvider.Request(event.aggregateId(), changed.total(), changed.paymentToken())));
        assertEquals(1, ledger.view(event.aggregateId()).orElseThrow().chargeCount());
    }
    @Test void expiredClaimsFenceStaleWorkersAndConcurrentProviderCallsHaveOneEffect() throws Exception {
        var event = event("tok_success"); payments.accept(event);
        var old = payments.claim().orElseThrow(); assertTrue(payments.claim().isEmpty());
        jdbc.update("UPDATE payment SET lease_until = now() - interval '1 second' WHERE payment_id = ?", event.aggregateId());
        var current = payments.claim().orElseThrow(); assertNotEquals(old.token(), current.token());
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var a = executor.submit(() -> provider.charge(charge(event))); var b = executor.submit(() -> provider.charge(charge(event)));
            var result = a.get(10, TimeUnit.SECONDS); assertEquals(result, b.get(10, TimeUnit.SECONDS));
            assertFalse(payments.finish(old, result)); payments.defer(old, "StaleWorker");
            assertEquals("UNKNOWN", view(event.aggregateId()).status());
            assertTrue(payments.finish(current, result)); assertFalse(payments.finish(old, result));
        }
        assertEquals(1, ledger.view(event.aggregateId()).orElseThrow().chargeCount());
        assertEquals(1, count("outbox_event", "aggregate_id", event.aggregateId()));
    }
    @Test void providerCommitSurvivesBusinessRollbackAndOutboxRecoversAtomically() {
        var rolledBack = event("tok_success");
        assertThrows(IllegalStateException.class, () -> tx.executeWithoutResult(status -> { payments.accept(rolledBack); throw new IllegalStateException("Crash"); }));
        assertEquals(0, count("payment", "payment_id", rolledBack.aggregateId())); assertEquals(0, count("processed_event", "event_id", rolledBack.eventId()));
        var event = event("tok_success"); payments.accept(event);
        jdbc.execute("ALTER TABLE outbox_event ADD CONSTRAINT test_failure CHECK (aggregate_id <> '" + event.aggregateId() + "'::uuid) NOT VALID");
        try { assertTrue(worker.processOne()); } finally { jdbc.execute("ALTER TABLE outbox_event DROP CONSTRAINT test_failure"); }
        assertEquals("UNKNOWN", view(event.aggregateId()).status()); assertEquals(0, view(event.aggregateId()).version());
        assertEquals(1, ledger.view(event.aggregateId()).orElseThrow().chargeCount()); assertEquals(0, count("outbox_event", "aggregate_id", event.aggregateId()));
        payments.retry(event.aggregateId()); assertTrue(worker.processOne());
        assertEquals("COMPLETED", view(event.aggregateId()).status()); assertEquals(1, ledger.view(event.aggregateId()).orElseThrow().requests());
        assertEquals(1, count("outbox_event", "aggregate_id", event.aggregateId()));
        assertThrows(IllegalStateException.class, () -> tx.executeWithoutResult(status -> provider.charge(charge(event))));
    }
    @Test void apiEnforcesOwnershipAndOmitsPrivatePaymentInstructions() throws Exception {
        var event = event("tok_success"); payments.accept(event); String path = "/api/payments/" + event.aggregateId();
        assertEquals(401, request("GET", path, null).statusCode());
        assertEquals(404, request("GET", path, "22222222-2222-2222-2222-222222222222").statusCode());
        var response = request("GET", path, CUSTOMER.toString()); assertEquals(200, response.statusCode());
        assertFalse(response.body().contains("paymentToken")); assertFalse(response.body().contains("shippingAddress"));
        assertEquals(403, request("GET", path + "/provider", CUSTOMER.toString()).statusCode());
        assertEquals(403, request("POST", path + "/retry", CUSTOMER.toString()).statusCode());
        assertEquals(200, request("POST", path + "/retry", "admin").statusCode());
        assertEquals(404, request("GET", path + "/provider", "admin").statusCode());
        worker.processOne(); assertEquals(200, request("GET", path + "/provider", "admin").statusCode());
        assertEquals(200, request("GET", path + "/outbox", "admin").statusCode());
        assertEquals(200, request("POST", path + "/retry", "admin").statusCode()); assertFalse(worker.processOne());
    }
    @Test @SuppressWarnings("unchecked") void realKafkaInputAndOutputPreserveIdentityAndRemoveFakeToken() throws Exception {
        var publisher = context.getBean(OutboxPublisher.class);
        for (int i = 0; i < 100 && publisher.publishOne(); i++) { }
        var event = event("tok_success"); KafkaTemplate<String, String> producer = context.getBean(KafkaTemplate.class);
        producer.send("inventory.events", event.aggregateId().toString(), json.writeValueAsString(event)).get(10, TimeUnit.SECONDS);
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (count("payment", "payment_id", event.aggregateId()) == 0 && System.nanoTime() < deadline) Thread.sleep(50);
        assertEquals("PENDING", view(event.aggregateId()).status()); worker.processOne();
        tx.executeWithoutResult(status -> { assertTrue(publisher.publishOne()); status.setRollbackOnly(); });
        assertEquals("PENDING", publisher.deliveries(event.aggregateId()).getFirst().status());
        assertTrue(publisher.publishOne());
        var props = new Properties(); props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString()); props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        try (var consumer = new KafkaConsumer<String, String>(props, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of("payment.events")); var received = new ArrayList<ConsumerRecord<String, String>>();
            deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (received.size() < 2 && System.nanoTime() < deadline) for (var row : consumer.poll(Duration.ofMillis(200))) if (event.aggregateId().toString().equals(row.key())) received.add(row);
            assertEquals(2, received.size()); assertEquals(received.getFirst().value(), received.getLast().value());
            var found = received.getFirst(); var output = json.readTree(found.value());
            assertEquals("PaymentCompleted", output.get("eventType").asString());
            assertEquals(event.eventId().toString(), output.get("causationId").asString());
            assertEquals(event.correlationId().toString(), output.get("correlationId").asString());
            assertEquals(event.traceparent(), new String(found.headers().lastHeader("traceparent").value(), StandardCharsets.UTF_8));
            assertFalse(found.value().contains("paymentToken")); assertNotNull(output.get("payload").get("shippingAddress"));
            assertEquals(publisher.deliveries(event.aggregateId()).getFirst().eventId().toString(), output.get("eventId").asString());
            assertEquals("PUBLISHED", publisher.deliveries(event.aggregateId()).getFirst().status());
        }
    }
    @Test void invalidSchemaAndKeyCannotCreatePaymentIntents() {
        var event = event("tok_success"); var listener = context.getBean(InventoryEventListener.class);
        assertThrows(IllegalArgumentException.class, () -> listener.receive(new ConsumerRecord<>("inventory.events", 0, 0, "bad-key", json.writeValueAsString(event))));
        for (String version : List.of("2", "1.5")) {
            String invalid = json.writeValueAsString(event).replace("\"schemaVersion\":1", "\"schemaVersion\":" + version);
            assertThrows(RuntimeException.class, () -> listener.receive(new ConsumerRecord<>("inventory.events", 0, 0, event.aggregateId().toString(), invalid)));
        }
        assertEquals(0, count("processed_event", "event_id", event.eventId())); assertEquals(0, count("payment", "payment_id", event.aggregateId()));
    }
    @Test void applicationRestartAutomaticallyReconcilesLostProviderResponse() throws Exception {
        var event = event("tok_timeout"); payments.accept(event); assertTrue(worker.processOne());
        assertEquals("UNKNOWN", view(event.aggregateId()).status());
        context.close(); startContext(true);
        try {
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (!view(event.aggregateId()).status().equals("COMPLETED") && System.nanoTime() < deadline) Thread.sleep(100);
            assertEquals("COMPLETED", view(event.aggregateId()).status());
            assertEquals(1, ledger.view(event.aggregateId()).orElseThrow().chargeCount());
            assertEquals(1, ledger.view(event.aggregateId()).orElseThrow().requests());
            assertEquals(1, count("outbox_event", "aggregate_id", event.aggregateId()));
        } finally { context.close(); startContext(false); }
    }
}
