package com.synechisveltiosi.orderservice;

import com.synechisveltiosi.orderservice.api.OrderDtos;
import com.synechisveltiosi.orderservice.domain.OrderAggregate;
import com.synechisveltiosi.orderservice.domain.OrderEvents;
import com.synechisveltiosi.orderservice.infrastructure.EventCodec;
import com.synechisveltiosi.platform.contracts.EventEnvelope;
import com.synechisveltiosi.platform.contracts.Money;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class OrderTest {
    static final UUID CUSTOMER = UUID.fromString("11111111-1111-1111-1111-111111111111");
    static OrderDtos.Create command(UUID id) {
        return new OrderDtos.Create(id, CUSTOMER, UUID.randomUUID(), 1L,
                List.of(new OrderEvents.Line(UUID.randomUUID(), "SKU-1", "Demo item", 2,
                        new Money(new BigDecimal("12.50"), Currency.getInstance("USD")))),
                "tok_success", new OrderEvents.Address("Demo Buyer", "1 Test Street", "Test City", "12345", "US"), null);
    }
    static EventEnvelope<OrderEvents.Event> envelope(UUID id, long version, OrderEvents.Event event) {
        return new EventEnvelope<>(UUID.randomUUID(), event instanceof OrderEvents.OrderCreated ? "OrderCreated" : "OrderNoteAdded",
                UUID.randomUUID(), UUID.randomUUID(), "Order", id, version, Instant.parse("2026-09-09T00:00:00Z"), 1, null, event);
    }
    @Test void reconstructsImmutableSnapshotAndNotesAndRejectsGaps() {
        var id = UUID.randomUUID();
        var created = envelope(id, 1, command(id).event());
        var note = envelope(id, 2, new OrderEvents.OrderNoteAdded(CUSTOMER, "Demo note"));
        var order = OrderAggregate.replay(id, List.of(created, note));
        assertEquals(2, order.version());
        assertEquals(new BigDecimal("25.00"), order.snapshot().total().amount());
        assertEquals("Demo note", order.notes().getFirst().text());
        assertThrows(UnsupportedOperationException.class, () -> order.snapshot().items().clear());
        assertThrows(IllegalStateException.class, () -> OrderAggregate.replay(id, List.of(note)));
        assertThrows(IllegalStateException.class, () -> OrderAggregate.replay(UUID.randomUUID(), List.of(created)));
        assertThrows(IllegalStateException.class, () -> OrderAggregate.replay(id, List.of(created, created)));
    }
    @Test void schemaOneDefaultsSalesChannelAndIgnoresFutureFields() {
        var codec = new EventCodec(JsonMapper.builder().build());
        var id = UUID.randomUUID();
        var json = codec.encode(envelope(id, 1, command(id).event()));
        var old = json.replace(",\"salesChannel\":\"WEB\"", "");
        assertFalse(old.contains("salesChannel"));
        assertEquals("WEB", ((OrderEvents.OrderCreated) codec.decode(old).payload()).salesChannel());
        assertEquals(codec.decode(json), codec.decode(json.replace("\"salesChannel\":\"WEB\"", "\"salesChannel\":\"WEB\",\"futureField\":true")));
        assertThrows(IllegalStateException.class, () -> codec.decode(json.replace("\"schemaVersion\":1", "\"schemaVersion\":2")));
    }
    // Represents a schema-1 consumer written before salesChannel was introduced.
    @com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
    record OriginalReader(UUID orderId, UUID customerId, Money total) { }

    @Test void checkedInFixturesWorkForOldAndNewReaders() throws Exception {
        var mapper = JsonMapper.builder().build(); var codec = new EventCodec(mapper);
        for (var variant : List.of("original", "additive")) {
            try (var input = getClass().getResourceAsStream("/contracts/order-created-v1-" + variant + ".json")) {
                assertNotNull(input);
                var json = new String(input.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                var current = (OrderEvents.OrderCreated) codec.decode(json).payload();
                var old = mapper.treeToValue(mapper.readTree(json).get("payload"), OriginalReader.class);
                assertEquals(current.orderId(), old.orderId());
                assertEquals(current.total(), old.total());
                assertEquals(variant.equals("original") ? "WEB" : "MOBILE", current.salesChannel());
            }
        }
    }
    @Test void boundsNotesAndRejectsInvalidPrices() {
        var id = UUID.randomUUID();
        var history = new ArrayList<EventEnvelope<OrderEvents.Event>>();
        history.add(envelope(id, 1, command(id).event()));
        for (int i = 0; i < 20; i++) history.add(envelope(id, i + 2, new OrderEvents.OrderNoteAdded(CUSTOMER, "Note")));
        assertThrows(IllegalArgumentException.class, () -> OrderAggregate.replay(id, history).addNote("Another"));
        assertThrows(IllegalArgumentException.class, () -> new OrderEvents.Line(UUID.randomUUID(), "SKU-1", "Item", 0,
                new Money(BigDecimal.ONE, Currency.getInstance("USD"))));
    }
}
