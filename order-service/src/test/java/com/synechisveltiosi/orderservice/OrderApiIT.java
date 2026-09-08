package com.synechisveltiosi.orderservice;

import com.synechisveltiosi.orderservice.api.OrderDtos;
import com.synechisveltiosi.orderservice.application.*;
import com.synechisveltiosi.orderservice.domain.OrderEvents;
import com.synechisveltiosi.orderservice.infrastructure.*;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerRecord;
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

import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class OrderApiIT {
    static final PostgreSQLContainer DB = new PostgreSQLContainer("postgres:17.6-alpine");
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1");
    static ConfigurableApplicationContext context;
    static JdbcTemplate jdbc;
    static OrderService orders;
    static EventCodec codec;
    static TransactionTemplate tx;
    static String base;
    static final String PASSWORD = "integration-test-only";

    @BeforeAll static void start() {
        DB.start(); KAFKA.start();
        context = SpringApplication.run(OrderServiceApplication.class, "--server.port=0", "--outbox.enabled=false",
                "--spring.datasource.url=" + DB.getJdbcUrl(), "--spring.datasource.username=" + DB.getUsername(),
                "--spring.datasource.password=" + DB.getPassword(), "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--demo.auth.customer-password=" + PASSWORD, "--demo.auth.second-customer-password=" + PASSWORD,
                "--demo.auth.admin-password=" + PASSWORD, "--demo.auth.checkout-password=" + PASSWORD);
        jdbc = context.getBean(JdbcTemplate.class); orders = context.getBean(OrderService.class); codec = context.getBean(EventCodec.class);
        tx = new TransactionTemplate(context.getBean(PlatformTransactionManager.class));
        base = "http://localhost:" + context.getEnvironment().getProperty("local.server.port");
    }
    @AfterAll static void stop() {
        if (context != null) context.close();
        KAFKA.stop(); DB.stop();
    }
    static CommandMetadata metadata() { return new CommandMetadata(UUID.randomUUID(), UUID.randomUUID(), null); }
    static UUID create() {
        var id = UUID.randomUUID(); orders.create(OrderTest.command(id), UUID.randomUUID(), metadata()); return id;
    }
    static HttpResponse<String> request(String method, String path, String user, Object body, UUID key) throws Exception {
        var request = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(15)).header("Content-Type", "application/json");
        if (user != null) request.header("Authorization", "Basic " + Base64.getEncoder().encodeToString((user + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8)));
        if (key != null) request.header("Idempotency-Key", key.toString());
        return HttpClient.newHttpClient().send(request.method(method, body == null ? HttpRequest.BodyPublishers.noBody() :
                HttpRequest.BodyPublishers.ofString(body instanceof String s ? s : codec.encode(body))).build(), HttpResponse.BodyHandlers.ofString());
    }
    @Test void apiOwnershipValidationReplayAndIdempotency() throws Exception {
        var command = OrderTest.command(UUID.randomUUID()); var key = UUID.randomUUID();
        assertEquals(401, request("POST", "/api/orders", null, command, key).statusCode());
        assertEquals(403, request("POST", "/api/orders", OrderTest.CUSTOMER.toString(), command, key).statusCode());
        var accepted = request("POST", "/api/orders", "checkout", command, key);
        assertEquals(202, accepted.statusCode(), accepted.body());
        assertEquals(accepted.body(), request("POST", "/api/orders", "checkout", command, key).body());
        assertEquals(409, request("POST", "/api/orders", "checkout", OrderTest.command(UUID.randomUUID()), key).statusCode());
        var path = "/api/orders/" + command.orderId();
        assertEquals(404, request("GET", path, "22222222-2222-2222-2222-222222222222", null, null).statusCode());
        assertEquals(403, request("GET", path + "/events", OrderTest.CUSTOMER.toString(), null, null).statusCode());
        assertEquals(400, request("POST", path + "/notes", OrderTest.CUSTOMER.toString(), "{\"expectedVersion\":1.5,\"note\":\"Test\"}", null).statusCode());
        assertEquals(200, request("POST", path + "/notes", OrderTest.CUSTOMER.toString(), new OrderDtos.AddNote(1L, "Test"), null).statusCode());
        assertEquals(409, request("POST", path + "/notes", OrderTest.CUSTOMER.toString(), new OrderDtos.AddNote(1L, "Stale"), null).statusCode());
        var view = request("GET", path, OrderTest.CUSTOMER.toString(), null, null);
        assertEquals(200, view.statusCode()); assertFalse(view.body().contains("paymentToken"));
        assertEquals(2, codec.read(view.body(), OrderDtos.View.class).version());
        assertEquals(2, orders.history(command.orderId()).size());
        assertEquals(200, request("GET", path + "/outbox", "admin", null, null).statusCode());
    }
    @Test void concurrentIdenticalCreatesAndExpectedVersionAppend() throws Exception {
        var command = OrderTest.command(UUID.randomUUID()); var key = UUID.randomUUID();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var barrier = new CyclicBarrier(2);
            Callable<OrderDtos.Accepted> create = () -> { barrier.await(10, TimeUnit.SECONDS); return orders.create(command, key, metadata()); };
            var a = executor.submit(create); var b = executor.submit(create);
            assertEquals(a.get(15, TimeUnit.SECONDS), b.get(15, TimeUnit.SECONDS));
            Callable<Boolean> append = () -> {
                try {
                    tx.executeWithoutResult(status -> {
                        var store = context.getBean(OrderEventStore.class); var loaded = store.load(command.orderId());
                        try { barrier.await(10, TimeUnit.SECONDS); } catch (Exception e) { throw new IllegalStateException(e); }
                        store.append(command.orderId(), loaded.version(), loaded.addNote("Concurrent"), metadata());
                    });
                    return true;
                } catch (ApiException conflict) { assertEquals("STALE_VERSION", conflict.code()); return false; }
            };
            var c = executor.submit(append); var d = executor.submit(append);
            assertNotEquals(c.get(15, TimeUnit.SECONDS), d.get(15, TimeUnit.SECONDS));
        }
        assertEquals(2, orders.history(command.orderId()).size());
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM outbox_event WHERE aggregate_id = ?", Integer.class, command.orderId()));
    }
    @Test void rollbackIsAtomicAndHistoryCannotBeRewritten() {
        var id = UUID.randomUUID();
        assertThrows(IllegalStateException.class, () -> tx.executeWithoutResult(status -> {
            orders.create(OrderTest.command(id), UUID.randomUUID(), metadata());
            throw new IllegalStateException("Simulated crash before commit");
        }));
        for (var table : List.of("order_stream", "domain_event", "outbox_event"))
            assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM " + table + " WHERE aggregate_id = ?", Integer.class, id));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM order_command WHERE order_id = ?", Integer.class, id));
        var existing = create();
        assertThrows(org.springframework.dao.DataAccessException.class, () -> jdbc.update("DELETE FROM domain_event WHERE aggregate_id = ?", existing));
        assertThrows(org.springframework.dao.DataAccessException.class, () -> jdbc.update("UPDATE outbox_event SET payload = '{}' WHERE aggregate_id = ?", existing));
        assertEquals(1, orders.history(existing).size());
    }
    @Test void publishesRealKafkaRecordsInStreamOrderWithStableIdentity() {
        var id = create(); orders.addNote(id, OrderTest.CUSTOMER, new OrderDtos.AddNote(1L, "Second event"), metadata());
        var props = new Properties(); props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString()); props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        try (var consumer = new KafkaConsumer<String, String>(props, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of("order.events"));
            var publisher = context.getBean(OutboxPublisher.class);
            for (int i = 0; i < 100 && publisher.publishOne(); i++) { }
            var received = new ArrayList<String>(); var deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (received.size() < 2 && System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(500))) if (id.toString().equals(record.key())) {
                    assertNotNull(record.headers().lastHeader("eventId")); received.add(record.value());
                }
            }
            assertEquals(2, received.size());
            for (int i = 0; i < 2; i++) {
                assertEquals(i + 1, codec.decode(received.get(i)).aggregateVersion());
                assertEquals(orders.history(id).get(i).eventId(), codec.decode(received.get(i)).eventId());
            }
            assertTrue(publisher.deliveries(id).stream().allMatch(row -> row.status().equals("PUBLISHED")));
        }
    }
    @Test @SuppressWarnings("unchecked") void competingPollerSkipsLockedStreamWithoutOvertaking() throws Exception {
        var real = context.getBean(OutboxPublisher.class);
        for (int i = 0; i < 100 && real.publishOne(); i++) { }
        var id = create(); orders.addNote(id, OrderTest.CUSTOMER, new OrderDtos.AddNote(1L, "Second"), metadata());
        var locked = new CountDownLatch(1); var release = new CountDownLatch(1);
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        var publisher = new OutboxPublisher(jdbc, kafka, codec, context.getBean(MeterRegistry.class));
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var holder = executor.submit(() -> tx.executeWithoutResult(status -> {
                jdbc.queryForList("SELECT id FROM outbox_event WHERE aggregate_id = ? AND aggregate_version = 1 FOR UPDATE", id);
                locked.countDown();
                try { if (!release.await(15, TimeUnit.SECONDS)) throw new IllegalStateException("Timed out waiting for competing poller"); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
            }));
            try {
                assertTrue(locked.await(10, TimeUnit.SECONDS));
                assertEquals(Boolean.FALSE, tx.execute(status -> publisher.publishOne()));
                var unrelated = create();
                assertEquals(Boolean.TRUE, tx.execute(status -> publisher.publishOne()));
                assertEquals("PUBLISHED", publisher.deliveries(unrelated).getFirst().status());
                verify(kafka, times(1)).send(any(ProducerRecord.class));
                assertTrue(publisher.deliveries(id).stream().allMatch(row -> row.attempts() == 0));
            } finally { release.countDown(); }
            holder.get(10, TimeUnit.SECONDS);
        }
    }
    @Test @SuppressWarnings("unchecked") void failedSendBlocksSuccessorAndAckBeforeRollbackPermitsDuplicate() {
        // Drain previous tests' pending rows so this test owns the next candidate.
        var real = context.getBean(OutboxPublisher.class);
        for (int i = 0; i < 100 && real.publishOne(); i++) { }
        var id = create(); orders.addNote(id, OrderTest.CUSTOMER, new OrderDtos.AddNote(1L, "Second"), metadata());
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("Broker unavailable")));
        var publisher = new OutboxPublisher(jdbc, kafka, codec, context.getBean(MeterRegistry.class));
        assertEquals(Boolean.TRUE, tx.execute(status -> publisher.publishOne()));
        assertEquals(Boolean.FALSE, tx.execute(status -> publisher.publishOne()));
        assertEquals(1, publisher.deliveries(id).getFirst().attempts());
        assertEquals(0, publisher.deliveries(id).getLast().attempts());
        jdbc.update("UPDATE outbox_event SET next_attempt_at = now() WHERE aggregate_id = ?", id);
        var sent = new ArrayList<String>();
        when(kafka.send(any(ProducerRecord.class))).thenAnswer(invocation -> {
            ProducerRecord<String, String> record = invocation.getArgument(0); sent.add(record.value());
            return CompletableFuture.completedFuture(null);
        });
        tx.executeWithoutResult(status -> { assertTrue(publisher.publishOne()); status.setRollbackOnly(); });
        assertEquals("PENDING", publisher.deliveries(id).getFirst().status());
        tx.executeWithoutResult(status -> assertTrue(publisher.publishOne()));
        assertEquals(sent.get(0), sent.get(1));
        tx.executeWithoutResult(status -> assertTrue(publisher.publishOne()));
        assertEquals(2, codec.decode(sent.get(2)).aggregateVersion());
    }
}
