package com.synechisveltiosi.shippingservice.api;
import com.synechisveltiosi.shippingservice.application.ShippingTransactions;
import com.synechisveltiosi.shippingservice.infrastructure.OutboxPublisher;
import org.springframework.web.bind.annotation.*;
import java.util.*;
@RestController
@RequestMapping("/api/shipping")
public class ShippingController {
    private final ShippingTransactions shipping; private final OutboxPublisher outbox;
    public ShippingController(ShippingTransactions shipping, OutboxPublisher outbox) { this.shipping = shipping; this.outbox = outbox; }
    @GetMapping("/{orderId}") public ShippingTransactions.View get(@PathVariable UUID orderId) { return shipping.get(orderId); }
    @GetMapping("/{orderId}/outbox") public List<OutboxPublisher.Delivery> outbox(@PathVariable UUID orderId) { shipping.get(orderId); return outbox.deliveries(orderId); }
}
