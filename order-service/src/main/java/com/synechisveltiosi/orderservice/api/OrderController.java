package com.synechisveltiosi.orderservice.api;

import com.synechisveltiosi.orderservice.application.CommandMetadata;
import com.synechisveltiosi.orderservice.application.OrderService;
import com.synechisveltiosi.orderservice.domain.OrderEvents;
import com.synechisveltiosi.orderservice.infrastructure.OutboxPublisher;
import com.synechisveltiosi.platform.contracts.EventEnvelope;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
public class OrderController {
    private final OrderService orders;
    private final OutboxPublisher outbox;
    public OrderController(OrderService orders, OutboxPublisher outbox) { this.orders = orders; this.outbox = outbox; }

    @PostMapping
    public ResponseEntity<OrderDtos.Accepted> create(@Valid @RequestBody OrderDtos.Create command,
            @RequestHeader("Idempotency-Key") UUID key, HttpServletRequest request) {
        var accepted = orders.create(command, key, metadata(request, key));
        return ResponseEntity.accepted().location(URI.create("/api/orders/" + accepted.orderId())).body(accepted);
    }

    @GetMapping("/{id}")
    public OrderDtos.View get(@PathVariable UUID id, Authentication authentication) {
        boolean admin = authentication.getAuthorities().stream().anyMatch(role -> role.getAuthority().equals("ROLE_ADMIN"));
        return orders.get(id, admin ? null : UUID.fromString(authentication.getName()), admin);
    }

    @PostMapping("/{id}/notes")
    public OrderDtos.View addNote(@PathVariable UUID id, @Valid @RequestBody OrderDtos.AddNote command,
                                  Authentication authentication, HttpServletRequest request) {
        return orders.addNote(id, UUID.fromString(authentication.getName()), command, metadata(request, UUID.randomUUID()));
    }

    @GetMapping("/{id}/events")
    public List<EventEnvelope<OrderEvents.Event>> history(@PathVariable UUID id) { return orders.history(id); }

    @GetMapping("/{id}/outbox")
    public List<OutboxPublisher.Delivery> deliveries(@PathVariable UUID id) {
        orders.get(id, null, true);
        return outbox.deliveries(id);
    }

    private CommandMetadata metadata(HttpServletRequest request, UUID cause) {
        return new CommandMetadata((UUID) request.getAttribute("correlationId"), cause, request.getHeader("traceparent"));
    }
}
